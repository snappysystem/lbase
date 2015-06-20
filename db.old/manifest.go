/*
Copyright (c) 2015, snappysystem
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

const (
	ManifestPrefix  = "manifest."
	SstPrefix       = "sst."
	kMaxRecordBytes = 1024 * 1024
)

// A list of requests
const (
	ManifestCreateFile byte = iota
	ManifestNewSnapshot
	ManifestMakeSnapshot
	ManifestDeleteSnapshot
	ManifestResetLog
	ManifestNewLog
)

// In-memory representation of each nsst file.
// File may be deleted if refcnt reaches 0 (i.e. no iterator
// refers to it, and there is no explicit snapshot request
// that needs it)
type FileInfo struct {
	Location string
	BeginKey []byte
	EndKey   []byte
	Refcnt   int
}

// Each time a new file is created or an old file is deleted,
// the system creates a new snapshot. Old snapshot will be
// deleted if its refcnt becomes 0 (i.e. no iterator refers
// to it, and there is no explicit snapshot request for it)
type SnapshotInfo struct {
	Levels [][]int64
	Refcnt int
}

// In memory representation of a manifest file. Each manifest
// file consist of an initial snapshot and logs of subsequent
// modifying request. On startup, old manifest file is read,
// logs in the file are replayed, and the resulting Manifest
// data structure is serialized to a new file as the base
// for next manifest file.
type ManifestData struct {
	FileMap      map[int64]FileInfo
	NextId       int64
	SnapshotMap  map[int64]SnapshotInfo
	NextSnapshot int64
	LogName      string
	// Current log file number.
	LogNumber int64
}

type Manifest struct {
	ManifestData
	env     Env
	rwMutex sync.RWMutex
	writer  *LogWriter
	// Unlike Refcnt field in SnapshotInfo, this map records
	// session based references. For example, if a user requests
	// to make a permanent snapshot, the Refcnt field in
	// SnapshotInfo should be incremented. If a user creates
	// an iterator, however, the refcnt in this map should
	// be incremented instead. All such references will be
	// gone once the application exits.
	snapshotRefcntMap map[int64]int
}

// Parse base file name, return its manifest number. If the base
// name does not fit into manifest file pattern, return -1 instead
func ParseManifestName(fname string) int64 {
	numPart := strings.TrimPrefix(fname, ManifestPrefix)
	if len(numPart) == len(fname) {
		return -1
	}

	numVal, err := strconv.ParseInt(numPart, 10, 64)
	if err != nil {
		return -1
	} else {
		return numVal
	}
}

func MakeManifestName(number int64) string {
	return fmt.Sprintf("%s%010d", ManifestPrefix, number)
}

func MakeSstName(number int64) string {
	return fmt.Sprintf("%s%010d", SstPrefix, number)
}

// Helper type to sort slice of int64
type int64Sortee []int64

func (x int64Sortee) Len() int           { return len(x) }
func (x int64Sortee) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
func (x int64Sortee) Less(i, j int) bool { return x[i] < x[j] }

// Return all manifest files in given directory @path. Then return
// full pathes of those files in ascending time order.
func ListAllManifestFiles(e Env, parent string) []string {
	lists, status := e.GetChildren(parent)
	if !status.Ok() {
		return []string{}
	}

	fileMap := make(map[int64]string)
	numList := make([]int64, 0, len(lists))

	for _, name := range lists {
		numVal := ParseManifestName(name)
		if numVal >= 0 {
			fileMap[numVal] = name
			numList = append(numList, numVal)
		}
	}

	sort.Sort(int64Sortee(numList))

	ret := make([]string, 0, len(numList))
	for _, num := range numList {
		val, ok := fileMap[num]
		if ok == true {
			ret = append(ret, path.Join(parent, val))
		}
	}

	return ret
}

func recoverSingleManifest(e Env, fullPath string) *Manifest {
	// first try to open the file
	ret := Manifest{env: e}
	file, status := e.NewSequentialFile(fullPath)
	if !status.Ok() {
		return nil
	}

	// read snapshot size from the file
	sizeBuf := make([]byte, 4)
	var dataReads []byte
	dataReads, status = file.Read(sizeBuf)
	if !status.Ok() {
		return nil
	}

	// read snapshot into buffer
	snapshotSize := *(*int32)(unsafe.Pointer(&dataReads[0]))
	dataSnapshot := make([]byte, snapshotSize)

	dataReads, status = file.Read(dataSnapshot)
	if !status.Ok() || len(dataReads) != int(snapshotSize) {
		return nil
	}

	// use gob to decode it
	buffer := bytes.NewBuffer(dataSnapshot)
	dec := gob.NewDecoder(buffer)
	err := dec.Decode(&ret)
	if err != nil {
		return nil
	}

	// Use a log reader to read logs.
	reader := MakeLogReader(e, fullPath, int64(snapshotSize+4), true)
	if reader == nil {
		return nil
	}

	tmpBuf := make([]byte, kMaxRecordBytes)
	for {
		readBuf, status := reader.ReadRecord(tmpBuf)
		if status == ReadStatusEOF {
			break
		} else if status != ReadStatusOk {
			return nil
		}

		replayBuf := bytes.NewBuffer(readBuf)
		dec := gob.NewDecoder(replayBuf)
		var action byte
		err := dec.Decode(&action)
		if err != nil {
			return nil
		}

		switch action {
		case ManifestCreateFile:
			ret.CreateFile(true)

		case ManifestNewSnapshot:
			var req NewSnapshotRequest
			err = dec.Decode(&req)
			if err != nil {
				return nil
			}

			ret.NewSnapshot(&req, true)

		case ManifestNewLog:
			var logNumber int64
			err = dec.Decode(&logNumber)
			if err != nil {
				return nil
			}

			ret.NewLog(logNumber, true)

		case ManifestMakeSnapshot:
			ret.MakeSnapshot(true)

		case ManifestDeleteSnapshot:
			var snapshot int64
			err = dec.Decode(&snapshot)
			if err != nil {
				return nil
			}

			ret.DeleteSnapshot(snapshot, true)

		case ManifestResetLog:
			var fname string
			err = dec.Decode(&fname)
			if err != nil {
				return nil
			}

			ret.ResetLog(fname, true)

		default:
			return nil
		}
	}

	return &ret
}

func initNewManifest(e Env, parent string) *Manifest {
	ret := &Manifest{
		ManifestData: ManifestData{
			FileMap:     make(map[int64]FileInfo),
			SnapshotMap: make(map[int64]SnapshotInfo),
		},
		env: e,
	}

	number := ret.CreateFile(true)
	base := MakeManifestName(number)
	fullPath := path.Join(parent, base)

	if !ret.saveAndInit([]string{}, fullPath) {
		return nil
	} else {
		return ret
	}
}

func RecoverManifest(e Env, parent string, createIfMissing bool) *Manifest {
	paths := ListAllManifestFiles(e, parent)
	var ret *Manifest
	for i := len(paths) - 1; i >= 0; i-- {
		fullPath := paths[i]
		if ret == nil {
			tmp := recoverSingleManifest(e, fullPath)
			if tmp != nil {
				ret = tmp
				break
			}
		}

		// remove corrupted or old manifest files
		e.DeleteFile(fullPath)
	}

	if ret == nil && createIfMissing {
		ret = initNewManifest(e, parent)
	} else if ret != nil {
		number := ret.CreateFile(true)
		base := MakeManifestName(number)
		fullPath := path.Join(parent, base)
		if !ret.saveAndInit(paths, fullPath) {
			return nil
		}
	}

	return ret
}

// create a new nsst file, return the file number.
func (m *Manifest) CreateFile(replay bool) int64 {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	ret := m.NextId
	m.NextId++

	// If this is not replay, write a log record.
	if !replay {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		enc.Encode(ManifestCreateFile)
		m.writer.AddRecord(buf.Bytes())
	}

	return ret
}

type NewSnapshotRequest struct {
	Levels [][]int64
	Files  map[int64]FileInfo
}

// Create a most recent snapshot. Return snapshot Id back. This is usually called
// after a merge (compaction)
func (m *Manifest) NewSnapshot(req *NewSnapshotRequest, replay bool) int64 {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	ret := m.NextSnapshot
	m.NextSnapshot++

	m.SnapshotMap[ret] = SnapshotInfo{
		Levels: req.Levels,
		Refcnt: 1,
	}

	// Add new files
	for id, info := range req.Files {
		orig, ok := m.FileMap[id]
		if !ok {
			m.FileMap[id] = info
		} else {
			orig.Refcnt++
			m.FileMap[id] = orig
		}
	}

	// Increase refcount for other files
	for _, flist := range req.Levels {
		for _, fnumber := range flist {
			// Make sure that it is not new file.
			if _, ok := req.Files[fnumber]; !ok {
				orig, check := m.FileMap[fnumber]
				if !check {
					panic("Expect file number does not exist!")
				}

				orig.Refcnt++
				m.FileMap[fnumber] = orig
			}
		}
	}

	// We have a new up-to-date snapshot, remove the previous one if possible.
	if ret > 0 {
		val := m.SnapshotMap[ret-1]
		val.Refcnt--

		if val.Refcnt == 0 {
			// If there is no pending iterators on previous snapshot,
			// remove it from snapshot map.
			count, _ := m.snapshotRefcntMap[ret-1]
			if count == 0 {
				m.removeSnapshotUnsafe(ret - 1)
			}
		}
	}

	if !replay {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		enc.Encode(ManifestNewSnapshot)
		enc.Encode(req)
		m.writer.AddRecord(buf.Bytes())
	}

	return ret
}

// Remember a new log number. This happens after a skiplist rotation.
func (m *Manifest) NewLog(logfileNumber int64, replay bool) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	m.LogNumber = logfileNumber

	if !replay {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		enc.Encode(ManifestNewLog)
		enc.Encode(logfileNumber)
		m.writer.AddRecord(buf.Bytes())
	}
}

// Add reference to the latest snapshot. This is usually used by an iterator
// to lock a snapshot (so that it will not be deleted after a new change
// in file set).
func (m *Manifest) AddRef() int64 {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	ret := m.NextSnapshot - 1
	val, _ := m.snapshotRefcntMap[ret]
	m.snapshotRefcntMap[ret] = val + 1

	return ret
}

// Give up reference to particular snapshot. This usually occurs when
// an iterator becomes out of scope.
func (m *Manifest) DeleteRef(snapshot int64) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	val, ok := m.snapshotRefcntMap[snapshot]
	if !ok {
		panic("Requested snapshot does not exist!")
	}

	val--

	if val > 0 {
		m.snapshotRefcntMap[snapshot] = val
	} else if val == 0 {
		delete(m.snapshotRefcntMap, snapshot)
		// If there is no persistent reference, remove associated files
		tmp, ok := m.SnapshotMap[snapshot]
		if !ok {
			panic("Fails to find snapshot!")
		}
		tmp.Refcnt--
		if tmp.Refcnt == 0 {
			m.removeSnapshotUnsafe(snapshot)
		} else {
			m.SnapshotMap[snapshot] = tmp
		}
	} else {
		panic("reference count becomes negative!")
	}
}

// Create a snapshot. After this operation, the system will not delete files that
// are included in the snapshot until it is removed.
func (m *Manifest) MakeSnapshot(replay bool) int64 {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	ret := m.NextSnapshot - 1
	val, ok := m.SnapshotMap[ret]
	if !ok {
		return ret
	}

	val.Refcnt++
	m.SnapshotMap[ret] = val

	if !replay {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		enc.Encode(ManifestMakeSnapshot)
		m.writer.AddRecord(buf.Bytes())
	}

	return ret
}

// Remove the snapshot made through previous MakeSnapshot() call.
func (m *Manifest) DeleteSnapshot(snapshot int64, replay bool) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	val, ok := m.SnapshotMap[snapshot]
	if !ok {
		panic("Fails to find snapshot!")
	}

	val.Refcnt--

	if val.Refcnt > 0 {
		m.SnapshotMap[snapshot] = val
	} else if val.Refcnt == 0 {
		_, ok = m.snapshotRefcntMap[snapshot]
		if !ok {
			m.removeSnapshotUnsafe(snapshot)
		}
	} else {
		panic("Refcnt should not be negative!")
	}

	if !replay {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		enc.Encode(ManifestDeleteSnapshot)
		enc.Encode(snapshot)
		m.writer.AddRecord(buf.Bytes())
	}
}

// Change log file name.
func (m *Manifest) ResetLog(fname string, replay bool) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	m.LogName = fname
	if !replay {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		enc.Encode(ManifestResetLog)
		enc.Encode(fname)
		m.writer.AddRecord(buf.Bytes())
	}
}

type FileInfoEx struct {
	FileInfo
	id int64
}

// Return the list of levels and files for a given snapshot
func (m *Manifest) GetSnapshotInfo(snapshot int64) [][]FileInfoEx {
	var ret [][]FileInfoEx
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	val, ok := m.SnapshotMap[snapshot]
	if !ok {
		return ret
	}

	for _, files := range val.Levels {
		var tmp []FileInfoEx
		for _, id := range files {
			info, found := m.FileMap[id]
			if !found {
				panic(fmt.Sprintf("Fails to find file info %d", id))
			}

			tmp = append(tmp, FileInfoEx{
				FileInfo: info,
				id:       id,
			})
		}

		ret = append(ret, tmp)
	}

	return ret
}

// Remove a snapshot from SnapshotMap, dereferencing all files in the snapshot.
// If file's reference count reaches 0, dereferance those files as well.
// The caller should lock mutex.
func (m *Manifest) removeSnapshotUnsafe(snapshot int64) {
	val, found := m.SnapshotMap[snapshot]
	if !found {
		panic("snapshot not found!")
	}

	for _, level := range val.Levels {
		for _, id := range level {
			fi, ok := m.FileMap[id]
			if !ok {
				panic("Unreferenced file id")
			}

			fi.Refcnt--

			// No one refers to the file, remove it
			if fi.Refcnt == 0 {
				m.env.DeleteFile(fi.Location)
				delete(m.FileMap, id)
			} else {
				m.FileMap[id] = fi
			}
		}
	}

	delete(m.SnapshotMap, snapshot)
}

func (m *Manifest) saveAndInit(allExistingFiles []string, fullPath string) bool {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(m)
	data := buf.Bytes()

	wf, status := m.env.NewWritableFile(fullPath)
	if !status.Ok() {
		return false
	}

	defer wf.Close()

	// File format: 4 bytes length at the beginning.
	tmp := make([]byte, 4)
	*(*int32)(unsafe.Pointer(&tmp[0])) = int32(len(data))

	status = wf.Append(tmp)
	if !status.Ok() {
		return false
	}

	// File format: followed by the snapshot.
	status = wf.Append(data)
	if !status.Ok() {
		return false
	}

	// After successfully commit the snapshot, there is no need for previous
	// manifest files. Remove all of them.
	status = wf.Flush()
	if status.Ok() {
		for _, fpath := range allExistingFiles {
			m.env.DeleteFile(fpath)
		}
	}

	// File format: followed by redo logs.
	m.writer = MakeLogWriter(m.env, fullPath)
	return true
}

// Get the most recent snapshot version
func (m *Manifest) GetCurrentSnapshot() int64 {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()
	return m.NextSnapshot - 1
}

// Get file information for @id.
func (m *Manifest) GetFileInfo(id int64) (FileInfo, bool) {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()
	info, found := m.FileMap[id]
	return info, found
}

// Close a manifest file. This method is only useful for testing purpose.
func (m *Manifest) Close() {
	m.writer.Close()
}
