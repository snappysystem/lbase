package db

import (
	"bytes"
	"encoding/gob"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

const (
	ManifestPrefix = "manifest_"
)

// A list of requests
const (
	ManifestCreateFile byte = iota
	ManifestNewSnapshot
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

	// use gob to decode it
	buffer := bytes.NewBuffer(dataSnapshot)
	dec := gob.NewDecoder(buffer)
	err := dec.Decode(&ret)
	if err != nil {
		return nil
	}

	return &ret
}

func initNewManifest(e Env, parent string) *Manifest {
	ret := Manifest{
		ManifestData: ManifestData{
			FileMap:     make(map[int64]FileInfo),
			SnapshotMap: make(map[int64]SnapshotInfo),
		},
		env: e,
	}

	return &ret
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
				continue
			}
		}

		// remove corrupted or old manifest files
		e.DeleteFile(fullPath)
	}

	if ret == nil && createIfMissing {
		ret = initNewManifest(e, parent)
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
func (m *Manifest) MakeSnapshot() int64 {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()

	ret := m.NextSnapshot - 1
	val, ok := m.SnapshotMap[ret]
	if !ok {
		return ret
	}

	val.Refcnt++
	m.SnapshotMap[ret] = val

	return ret
}

// Remove the snapshot made through previous MakeSnapshot() call.
func (m *Manifest) DeleteSnapshot(snapshot int64) {
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
}

// Return the list of levels and files for a given snapshot
func (m *Manifest) GetSnapshotInfo(snapshot int64) [][]FileInfo {
	var ret [][]FileInfo
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()

	val, ok := m.SnapshotMap[snapshot]
	if !ok {
		return ret
	}

	for _, files := range val.Levels {
		var tmp []FileInfo
		for _, id := range files {
			info, found := m.FileMap[id]
			if !found {
				panic("Fails to find file info!")
			}

			tmp = append(tmp, info)
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
