package server

import (
	"os"
	"testing"
)

func initStorageForTest(root string) *RaftStorage {
	os.RemoveAll(root)

	logRoot := root + "/log"
	storeRoot := root + "/store"

	dirErr := os.MkdirAll(logRoot, os.ModePerm)
	if dirErr != nil {
		panic("Fails to create log dir")
	}

	dirErr = os.MkdirAll(storeRoot, os.ModePerm)
	if dirErr != nil {
		panic("Fails to create store dir")
	}

	raftOpts := RaftOptionsForTest(logRoot)
	storeOpts := &RegionStoreOptions{Name: storeRoot}

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
	store := initStorageForTest(root)

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
	store := initStorageForTest(root)

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
	store := initStorageForTest(root)

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
