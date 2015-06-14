package server

import (
	"lbase/balancer"
)

type RaftOptions struct {
	// An ID uniquely identify this raft quorum.
	Region balancer.Region
	// Members of this raft quorum.
	Members []balancer.ServerName
	// The network access point of myself.
	Address balancer.ServerName
	// Path to log db.
	LogDbRoot string
	// If candidate cannot proceed, how long in millisecond
	// the candidate should wait until retry again.
	CandidateWaitMs int64
	// Timeout value for RequestVote call.
	RequestVoteTimeoutMs int64
	// Timeout value for leader.
	RaftLeaderTimeoutMs int64
	// HTTP RPC path prefix.
	RPCPrefix string
	// How to collect incoming raft records.
	Collector RecordCollector
}

func DefaultRaftOptions(root string) *RaftOptions {
	return &RaftOptions{
		LogDbRoot:            root,
		CandidateWaitMs:      4000,
		RequestVoteTimeoutMs: 2000,
		RaftLeaderTimeoutMs:  60000,
	}
}

func RaftOptionsForTest(root string) *RaftOptions {
	return &RaftOptions{
		LogDbRoot:            root,
		CandidateWaitMs:      800,
		RequestVoteTimeoutMs: 400,
		RaftLeaderTimeoutMs:  200,
	}
}
