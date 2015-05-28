package server

import (
	"lbase/balancer"
)

// Raft protocol command.
type RequestVote struct {
	// The region where raft vote occurs.
	Region balancer.Region
	// The candidate's identity.
	ServerName balancer.ServerName
	// The next term that the candidate want to apply.
	Term int64
	// The sequence value of the candidate.
	LastSequence RaftSequence
}

// The response for a RequestVote request.
type RequestVoteReply struct {
	// Wether or not the server agrees that the candiate should be leader.
	Ok bool
	// If the caller has smaller term number, return current term back
	// so that the caller can have a chance to catch up in voting.
	MyTerm int64
}
