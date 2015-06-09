package server

import (
	"lbase/balancer"
)

// Raft protocol command.
type AppendEntries struct {
	ServerName balancer.ServerName
	// Current term.
	Term int64
	// Id of this region.
	Region balancer.Region
	// Sequence number that the leader believes the server has.
	LeaderGuessedSequence RaftSequence
	// List of records that the leader wants to push to other servers.
	Data map[RaftSequence][]byte
}

// The response to raft protocol command.
type AppendEntriesReply struct {
	// Set it to true if the server believes the leader is no
	// longer the current leader for the region.
	NotLeader bool
	// If leader and server has already agreed on the sequence
	// number, return the next sequence to be replicated from
	// the leader;
	// The same as LeaderGuessedSequence if the leader guessed
	// correctly, otherwise return the previous sequence number
	// for leader to match.
	RealSequence RaftSequence
}
