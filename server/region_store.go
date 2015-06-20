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
