package db

import (
	"sync"
)

type DbImpl struct {
	path     string
	env      Env
	comp     Comparator
	skipList *Skiplist
	tmpList  *Skiplist
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
