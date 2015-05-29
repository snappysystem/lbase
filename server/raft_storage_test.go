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
