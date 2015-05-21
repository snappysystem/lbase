package server

import (
	"lbase/balancer"
)

type RaftOptions struct {
	// An ID uniquely identify this raft quorum.
	RaftId string
	// Members of this raft quorum.
	Members []balancer.ServerName
	// The network access point of myself.
	Address balancer.ServerName
}
