package server

import (
	"lbase/balancer"
	"testing"
)

func initRaftStates(
	root string,
	reg balancer.Region,
	recreate bool) (states *RaftStates, server *Server) {

	store := initRaftStorageForTest(root, reg, recreate)
	if store == nil {
		return
	}

	opts := store.GetRaftOptions()
	states = NewRaftStates(opts, store)
	server, _ = NewServerAndPort()

	if server != nil && states != nil {
		server.RegisterRegion(reg, states)
	}

	return
}

func TestRaftStatesInitialized(t *testing.T) {
	root := "/tmp/TestRaftStatesInitialized"
	reg := balancer.Region{}

	states, server := initRaftStates(root, reg, true)
	if server == nil {
		t.Error("Fails to create a server!")
	}

	defer server.Close()

	if states == nil {
		t.Error("Fails to create a raft states!")
	}
}
