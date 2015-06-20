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
