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
		panic("Fails to open db")
	}

	return &RegionStore{
		opts:   ropts,
		db:     leveldb,
		wrOpts: db.NewWriteOptions(),
	}
}

func (s *RegionStore) Put(key, value []byte, seq RaftSequence) {
	sKey := NewStoreKey(key, seq.Index)
	err := s.db.Put(s.wrOpts, sKey, value)
	if err != nil {
		panic(fmt.Sprintf("Put: %#v", err))
	}
}
