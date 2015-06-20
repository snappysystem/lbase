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
	"lbase/balancer"
	"os"
	"testing"
)

func initRaftStorageForTest(root string, reg balancer.Region, hard bool) *RaftStorage {
	if hard {
		os.RemoveAll(root)
	}

	logRoot := root + "/log"
	storeRoot := root + "/store"

	if hard {
		dirErr := os.MkdirAll(logRoot, os.ModePerm)
		if dirErr != nil {
			panic("Fails to create log dir")
		}

		dirErr = os.MkdirAll(storeRoot, os.ModePerm)
		if dirErr != nil {
			panic("Fails to create store dir")
		}
	}

	raftOpts := RaftOptionsForTest(logRoot)
	storeOpts := &RegionStoreOptions{Name: storeRoot, Region: reg}

	regionStore := NewRegionStore(storeOpts)
	if regionStore == nil {
		panic("Fails to create a store")
	}

	raftStore, err := NewRaftStorage(raftOpts, regionStore)
	if err != nil {
		panic("Fails to create raftStore")
	}

	return raftStore
}

func TestInitRaftStorage(t *testing.T) {
	root := "/tmp/TestRaftStorage"
	store := initRaftStorageForTest(root, balancer.Region{}, true)

	seq := store.GetRaftSequence()
	if seq.Index != 0 || seq.Term != 0 {
		t.Error("expect sequence 0 does not appear:", seq)
	}

	seq = store.GetCommitSequence()
	if seq.Index != 0 || seq.Term != 0 {
		t.Error("expect sequence 0 does not appear:", seq)
	}
}

func TestSaveRaftRecordNonOverride(t *testing.T) {
	root := "/tmp/TestSaveRaftRecordNonOverride"
	store := initRaftStorageForTest(root, balancer.Region{}, true)

	s1 := RaftSequence{Index: 1, Term: 1}
	store.SaveRaftRecord(s1, []byte("hello"))

	s2 := RaftSequence{Index: 2, Term: 1}
	store.SaveRaftRecord(s2, []byte("world"))

	seq := store.GetRaftSequence()
	if seq.Index != 2 || seq.Term != 1 {
		t.Error("expect sequence 0 does not appear:", seq)
	}

	seq = store.GetCommitSequence()
	if seq.Index != 0 || seq.Term != 0 {
		t.Error("expect sequence 0 does not appear:", seq)
	}
}

func TestSaveRaftRecordOverride(t *testing.T) {
	root := "/tmp/TestSaveRaftRecordOverride"
	store := initRaftStorageForTest(root, balancer.Region{}, true)

	s1 := RaftSequence{Index: 1, Term: 1}
	store.SaveRaftRecord(s1, []byte("hello"))

	s2 := RaftSequence{Index: 1, Term: 1}
	store.SaveRaftRecord(s2, []byte("world"))

	seq := store.GetRaftSequence()
	if seq.Index != 1 || seq.Term != 1 {
		t.Error("expect sequence 0 does not appear:", seq)
	}

	seq = store.GetCommitSequence()
	if seq.Index != 0 || seq.Term != 0 {
		t.Error("expect sequence 0 does not appear:", seq)
	}
}

func TestCommitRaftLog(t *testing.T) {
	root := "/tmp/TestCommitRaftLog"
	store := initRaftStorageForTest(root, balancer.Region{}, true)

	r1 := RaftRecord{Key: []byte("one"), Value: []byte("hello")}
	s1 := RaftSequence{Index: 1, Term: 1}
	store.SaveRaftRecord(s1, r1.ToSlice())

	r2 := RaftRecord{Key: []byte("two"), Value: []byte("world")}
	s2 := RaftSequence{Index: 2, Term: 1}
	store.SaveRaftRecord(s2, r2.ToSlice())

	store.Commit(RaftSequence{Index: 1, Term: 1})

	seq := store.GetRaftSequence()
	if seq.Index != 2 || seq.Term != 1 {
		t.Error("expect sequence 0 does not appear:", seq)
	}

	seq = store.GetCommitSequence()
	if seq.Index != 1 || seq.Term != 1 {
		t.Error("expect sequence 0 does not appear:", seq)
	}
}

func TestRaftStorageSimpleReload(t *testing.T) {
	root := "/tmp/TestRaftStorageSimpleReload"

	{
		store := initRaftStorageForTest(root, balancer.Region{}, true)

		r1 := RaftRecord{Key: []byte("one"), Value: []byte("hello")}
		s1 := RaftSequence{Index: 1, Term: 1}
		store.SaveRaftRecord(s1, r1.ToSlice())

		r2 := RaftRecord{Key: []byte("two"), Value: []byte("world")}
		s2 := RaftSequence{Index: 2, Term: 1}
		store.SaveRaftRecord(s2, r2.ToSlice())

		store.Commit(RaftSequence{Index: 1, Term: 1})
		store.Close()
	}

	{
		store := initRaftStorageForTest(root, balancer.Region{}, false)

		seq := store.GetRaftSequence()
		if seq.Index != 2 || seq.Term != 1 {
			t.Error("expect sequence 0 does not appear:", seq)
		}

		seq = store.GetCommitSequence()
		if seq.Index != 1 || seq.Term != 1 {
			t.Error("expect sequence 0 does not appear:", seq)
		}
	}
}
