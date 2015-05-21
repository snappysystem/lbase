package server

import (
	"lbase/balancer"
)

// Raft protocol command.
type RequestVote struct {
	// The candidate's identity.
	ServerName balancer.ServerName
	// The next term that the candidate want to apply.
	Term int64
	// The sequence value of the candidate.
	LastSequence RaftSequence
}

// The response for a RequestVote request.
type RequestVoteResponse struct {
	// Wether or not the server agrees that the candiate should be leader.
	Ok bool
}
