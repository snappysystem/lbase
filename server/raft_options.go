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
	"fmt"
	"lbase/balancer"
)

type RaftOptions struct {
	// An ID uniquely identify this raft quorum.
	Region balancer.Region
	// Members of this raft quorum.
	Members []balancer.ServerName
	// The network access point of myself.
	Address balancer.ServerName
	// File Path to save raft data.
	RaftRoot string
	// If candidate cannot proceed, how long in millisecond
	// the candidate should wait until retry again.
	CandidateWaitMs int64
	// Timeout value for RequestVote call.
	RequestVoteTimeoutMs int64
	// Timeout value for leader.
	RaftLeaderTimeoutMs int64
	// HTTP RPC path prefix.
	RPCPrefix string
	// If this is nil, RaftStates will create one.
	EditQueue *EditQueue
	// If this is nil, RaftStates will create one.
	Collector *EditCollector
}

func DefaultRaftOptions(root string) *RaftOptions {
	return &RaftOptions{
		RaftRoot:             root,
		CandidateWaitMs:      4000,
		RequestVoteTimeoutMs: 2000,
		RaftLeaderTimeoutMs:  60000,
	}
}

func RaftOptionsForTest(root string) *RaftOptions {
	return &RaftOptions{
		RaftRoot:             root,
		CandidateWaitMs:      800,
		RequestVoteTimeoutMs: 400,
		RaftLeaderTimeoutMs:  200,
	}
}

func (opts *RaftOptions) GetLogDir() string {
	return fmt.Sprintf("%s/log", opts.RaftRoot)
}

func (opts *RaftOptions) GetEditQueueDir() string {
	return fmt.Sprintf("%s/editq", opts.RaftRoot)
}
