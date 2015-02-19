package db

import (
	"container/list"
	"sync"
)

// A FIFO queue for table cache.
type TableQueue struct {
	list     *list.List
	capacity int
}

// Add a new table Id into queue. Return the oldest one in the queue, or
// -1 if there are less than @capacity elements in the queue
func (tq *TableQueue) Add(id int64) (int64, *list.Element) {
	element := &list.Element{Value: id}
	tq.list.PushFront(element)
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
		queue:    TableQueue{capacity: capacity},
		impl:     impl,
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

type DbImpl struct {
	path     string
	env      Env
	comp     Comparator
	skipList *Skiplist
	tmpList  *Skiplist
	manifest *Manifest
	tblCache *TableCache
	mutex    sync.RWMutex
}

// Freeze current skiplist, push it down to tmpList, create a new skiplist
func (db *DbImpl) RotateSkiplist() (*Skiplist, *Skiplist) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.tmpList != nil {
		panic("tmpList is not empty during rotation!")
	}

	db.tmpList = db.skipList
	db.skipList = MakeSkiplist()

	return db.skipList, db.tmpList
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
