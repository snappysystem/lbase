package server

import (
	"fmt"
	"lbase/db"
)

const (
	RAFT_FOLLOWER = iota
	RAFT_CANDIDATE
	RAFT_LEADER
)

type RaftStates struct {
	// Raft state.
	state int
	opts  *RaftOptions
	// Underlying storage.
	db *db.Db
}

func NewRaftStates(opts *RaftOptions, db *db.Db) *RaftStates {
	return &RaftStates{
		state: RAFT_FOLLOWER,
		opts:  opts,
		db:    db,
	}
}

func (s *RaftStates) TransitToCandidate() {
	if s.state != RAFT_FOLLOWER {
		panic(fmt.Sprintf("current state is not follower: %#v", s.state))
	}
}
