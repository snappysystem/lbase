package db

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"path"
	"sync"
)

// Parameters about DB.
type DbOption struct {
	path         string
	env          Env
	comp         Comparator
	numTblCache  int
	minLogSize   int64
	maxL0Levels  int
	minTableSize int64
}

// A FIFO queue for table cache.
type TableQueue struct {
	list     *list.List
	capacity int
}

// Add a new table Id into queue. Return the oldest one in the queue, or
// -1 if there are less than @capacity elements in the queue
func (tq *TableQueue) Add(id int64) (int64, *list.Element) {
	element := tq.list.PushFront(id)
	if tq.list.Len() < tq.capacity {
		return -1, element
	} else {
		tmp := tq.list.Back()
		ret := tmp.Value.(int64)
		tq.list.Remove(tmp)
		return ret, element
	}
}

// Move the accessed element to the front of the queue
func (tq *TableQueue) Access(element *list.Element) {
	tq.list.MoveToFront(element)
}

type tblInfo struct {
	table   *Table
	element *list.Element
	done    chan bool
}

// Manage cache of tables.
type TableCache struct {
	mutex    sync.Mutex
	tableMap map[int64]tblInfo
	queue    TableQueue
	impl     *DbImpl
}

func MakeTableCache(impl *DbImpl, capacity int) *TableCache {
	return &TableCache{
		tableMap: map[int64]tblInfo{},
		queue: TableQueue{
			list:     list.New(),
			capacity: capacity,
		},
		impl: impl,
	}
}

// Add a table into cache.
func (tc *TableCache) Add(table *Table, id int64) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	_, found := tc.tableMap[id]
	if found {
		panic("New table should not be in the cache!")
	}

	oldId, element := tc.queue.Add(id)
	tc.tableMap[id] = tblInfo{table: table, element: element, done: make(chan bool)}
	if oldId >= 0 {
		delete(tc.tableMap, oldId)
	}
}

func (tc *TableCache) Get(id int64) *Table {
	tc.mutex.Lock()

	// Find if the table is already in cache.
	info, found := tc.tableMap[id]
	if found && info.table != nil {
		tc.queue.Access(info.element)
		tc.mutex.Unlock()
		return info.table
	}

	// Someone else is trying to load the table, so let's wait.
	if found && info.table == nil {
		tc.mutex.Unlock()
		<-info.done
		return tc.Get(id)
	}

	// Cache item cannot be found, put a place holder so that
	// other go routine that uses it can wait.
	tc.tableMap[id] = tblInfo{done: make(chan bool)}
	tc.mutex.Unlock()

	finfo, found := tc.impl.GetManifest().GetFileInfo(id)
	if !found {
		panic("Expected id does not found")
	}

	file, status := tc.impl.GetEnv().NewSequentialFile(finfo.Location)
	if !status.Ok() {
		panic("File does not exist!")
	}

	var fsize uint64
	fsize, status = tc.impl.GetEnv().GetFileSize(finfo.Location)
	if !status.Ok() {
		panic("Cannot stat a file!")
	}

	buf := make([]byte, fsize)
	tbl := RecoverTable(file, buf, tc.impl.GetComparator())

	if tbl == nil {
		panic("Fails to recover a table!")
	}

	// Add the table into cache.
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	info, found = tc.tableMap[id]
	if !found {
		panic("Fails to find previously reserved entry!")
	}

	oldId, element := tc.queue.Add(id)

	info.table = tbl
	info.element = element

	tc.tableMap[id] = info

	// Notify all pending go routines that this table is loaded.
	close(info.done)

	if oldId >= 0 {
		delete(tc.tableMap, oldId)
	}

	return tbl
}

// The real DB type.
type DbImpl struct {
	path           string
	env            Env
	comp           Comparator
	writer         *LogWriter
	previousWriter *LogWriter
	skipList       *Skiplist
	tmpList        *Skiplist
	manifest       *Manifest
	tblCache       *TableCache
	compactor      *Compactor
	minLogSize     int64
	compacting     bool
	mutex          sync.RWMutex
}

// Create a brand new Db.
func MakeDb(opt DbOption) *DbImpl {
	opt.env.DeleteDir(opt.path)
	status := opt.env.CreateDir(opt.path)
	if !status.Ok() {
		return nil
	}

	// Create a new manifest.
	manifest := RecoverManifest(opt.env, opt.path, true)
	if manifest == nil {
		return nil
	}

	// Create first log file
	newLogFileNumber := manifest.CreateFile(false)
	logBaseName := GetLogName(newLogFileNumber)
	newLogPath := path.Join(opt.path, logBaseName)
	writer := MakeLogWriter(opt.env, newLogPath)
	if writer == nil {
		return nil
	}

	ret := &DbImpl{
		path:       opt.path,
		env:        opt.env,
		comp:       opt.comp,
		writer:     writer,
		skipList:   MakeSkiplist(opt.comp),
		manifest:   manifest,
		minLogSize: opt.minLogSize,
		compacting: false,
	}

	ret.tblCache = MakeTableCache(ret, opt.numTblCache)
	ret.compactor = MakeCompactor(ret, opt)

	return ret
}

// Update an entry and return a channel so that caller can wait for
// the completion of a L0 compaction. If no compaction is triggered
// for this put, return a nil channel.
func (db *DbImpl) PutMore(opt WriteOptions, key, value []byte) (Status, chan bool) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(key)
	enc.Encode(value)

	db.mutex.Lock()
	db.skipList.Put(key, value)
	status := db.writer.AddRecord(buf.Bytes())
	db.mutex.Unlock()

	var rc chan bool

	// Check if L0 compaction is needed.
	logSize := db.writer.file.Size()
	if logSize > db.minLogSize {
		db.mutex.Lock()
		allowCompaction := false
		if !db.compacting {
			db.compacting = true
			allowCompaction = true
		}
		db.mutex.Unlock()

		if allowCompaction {
			rc = db.compactor.StartL0Compaction()
		}
	}

	return status, rc
}

func (db *DbImpl) Put(opt WriteOptions, key, value []byte) Status {
	ret, _ := db.PutMore(opt, key, value)
	return ret
}

func (db *DbImpl) Delete(opt WriteOptions, key []byte) Status {
	return db.Put(opt, key, []byte(""))
}

func (db *DbImpl) Write(opt WriteOptions, updates WriteBatch) Status {
	return MakeStatusNotFound(NOT_IMPLEMENTED)
}

func (db *DbImpl) Get(opt ReadOptions, key []byte) ([]byte, Status) {
	iter := db.NewIterator(opt)
	iter.Seek(key)
	defer iter.Close()

	if iter.Valid() && db.comp.Compare(iter.Key(), key) == 0 {
		return iter.Value(), MakeStatusOk()
	} else {
		return nil, MakeStatusNotFound(KEY_NOT_FOUND)
	}
}

func (db *DbImpl) NewIterator(opt ReadOptions) Iterator {
	iterList := make([]Iterator, 0)

	db.mutex.RLock()
	defer db.mutex.RUnlock()

	iterList = append(iterList, db.skipList.NewIterator(&opt))

	if db.tmpList != nil {
		iterList = append(iterList, db.tmpList.NewIterator(&opt))
	}

	sId := db.manifest.GetCurrentSnapshot()
	sinfo := db.manifest.GetSnapshotInfo(sId)

	// Add iterators from L0 tables.
	for idx, infos := range sinfo {
		if idx >= db.compactor.maxL0Levels {
			break
		}

		// Skip empty L0 layers.
		if len(infos) == 0 {
			continue
		}

		tid := infos[0].id
		tbl := db.GetTableCache().Get(tid)
		if tbl == nil {
			panic("Expected table is not found")
		}

		iter := tbl.NewIterator()
		iterList = append(iterList, iter)
	}

	// Add iterators from Ln tables.
	for i := db.compactor.maxL0Levels; i < len(sinfo); i++ {
		iter := MakeBinarySearchIterator(db, sinfo[i])
		iterList = append(iterList, iter)
	}

	return MakeHeapIterator(iterList, db.comp)
}

func (db *DbImpl) GetSnapshot() Snapshot {
	panic(NOT_IMPLEMENTED)
	return 0
}

func (db *DbImpl) ReleaseSnapshot(snap Snapshot) {
	panic(NOT_IMPLEMENTED)
}

func (db *DbImpl) GetApproximateSizes(ranges []Range) []uint64 {
	panic(NOT_IMPLEMENTED)
	return nil
}

func (db *DbImpl) CompactRange(start, limit []byte) {
	panic(NOT_IMPLEMENTED)
}

// Freeze current skiplist, push it down to tmpList, create a new skiplist
func (db *DbImpl) RotateSkiplist() (*Skiplist, *Skiplist) {
	// Create a new log file.
	newLogFileNumber := db.manifest.CreateFile(false)
	logBaseName := GetLogName(newLogFileNumber)
	newLogPath := path.Join(db.path, logBaseName)
	writer := MakeLogWriter(db.env, newLogPath)
	if writer == nil {
		panic("Fails to create a new log file")
	}

	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.tmpList != nil {
		panic("tmpList is not empty during rotation!")
	}

	if db.previousWriter != nil {
		panic("Previous writer has not been cleared yet!")
	}

	db.previousWriter = db.writer
	db.writer = writer
	db.tmpList = db.skipList
	db.skipList = MakeSkiplist()
	db.compacting = false

	return db.skipList, db.tmpList
}

func (db *DbImpl) CompactionDone(newReq *NewSnapshotRequest) {
	db.mutex.Lock()

	db.tmpList = nil
	db.manifest.NewSnapshot(newReq, false)

	previousWriter := db.previousWriter
	db.previousWriter = nil

	db.mutex.Unlock()

	db.env.DeleteFile(previousWriter.Name())
}

// Get parent dir of this db.
func (db *DbImpl) GetPath() string {
	return db.path
}

func (db *DbImpl) GetEnv() Env {
	return db.env
}

func (db *DbImpl) GetComparator() Comparator {
	return db.comp
}

func (db *DbImpl) GetManifest() *Manifest {
	return db.manifest
}

func (db *DbImpl) GetTableCache() *TableCache {
	return db.tblCache
}
