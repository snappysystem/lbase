package server

import (
	"fmt"
	"lbase/balancer"
	"lbase/db"
)

type RegionStoreOptions struct {
	// Path to the data store.
	Name string
	// Identity of this region store.
	Region balancer.Region
}

type RegionStore struct {
	opts   *RegionStoreOptions
	db     db.Db
	wrOpts db.WriteOptions
}

func NewRegionStore(ropts *RegionStoreOptions) *RegionStore {
	opts := db.NewDbOptions()
	opts.SetCreateIfMissing(1)

	leveldb, openError := db.OpenDb(opts, ropts.Name)
	if openError != nil {
		panic(fmt.Sprintf("Fails to open db:%#v", openError))
	}

	return &RegionStore{
		opts:   ropts,
		db:     leveldb,
		wrOpts: db.NewWriteOptions(),
	}
}

func (s *RegionStore) Put(key, value []byte, ver int64) {
	sKey := NewStoreKey(key, ver)
	err := s.db.Put(s.wrOpts, sKey, value)
	if err != nil {
		panic(fmt.Sprintf("Put: %#v", err))
	}
}

func (s *RegionStore) GetDb() db.Db {
	return s.db
}

// Flush all data and invalidate the objects.
func (s *RegionStore) Close() {
	s.db.Close()
}
