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

type ServerRPC struct {
	regionRaftMap  map[balancer.Region]*RaftStates
	regionQueueMap map[balancer.Region]*EditQueue
}

func (s *ServerRPC) init() {
	s.regionRaftMap = make(map[balancer.Region]*RaftStates)
	s.regionQueueMap = make(map[balancer.Region]*EditQueue)
}

// A simple RPC method to test if the server is alive.
func (s *ServerRPC) Echo(x int, resp *int) error {
	*resp = x
	return nil
}

func (s *ServerRPC) RequestVote(req RequestVote, resp *RequestVoteReply) error {
	states, found := s.regionRaftMap[req.Region]
	if found {
		states.HandleRequestVote(&req, resp)
	}
	return nil
}

func (s *ServerRPC) AppendEntries(req AppendEntries, resp *AppendEntriesReply) error {
	states, found := s.regionRaftMap[req.Region]
	if found {
		states.HandleAppendEntries(&req, resp)
	}
	return nil
}

// Request to get the state of a raft member.
type RaftStateRequest struct {
	Region balancer.Region
}

type RaftStateReply struct {
	Found bool
	State int
}

func (s *ServerRPC) GetRaftState(req RaftStateRequest, resp *RaftStateReply) error {
	states, found := s.regionRaftMap[req.Region]
	if found {
		states.HandleGetRaftState(req, resp)
	}
	return nil
}

// Request to add data to pending queue.
type AddDataRequest struct {
	Region balancer.Region
	Data   []byte
}

type AddDataReply struct {
	Ok bool
}

func (s *ServerRPC) AddData(req *AddDataRequest, resp *AddDataReply) error {
	queue, found := s.regionQueueMap[req.Region]
	if found {
		queue.Put(req.Data)
		resp.Ok = true
	}
	return nil
}

// Request to get N records from a member of quorum.
type GetNRecordsRequest struct {
	Region          balancer.Region
	StartSequence   int64
	NumberOfRecords int
}

type GetNRecordsReply struct {
	Ok      bool
	Records [][]byte
}

func (s *ServerRPC) GetNRecords(
	req *GetNRecordsRequest,
	resp *GetNRecordsReply) error {

	queue, found := s.regionQueueMap[req.Region]
	if found {
		var seq int64
		resp.Records, seq = queue.GetN(req.StartSequence, req.NumberOfRecords)
		if seq > 0 {
			resp.Ok = true
		}
	}
	return nil
}

// Trim pending queue to the required sequence number.
type TrimEditQueueRequest struct {
	Region      balancer.Region
	EndSequence int64
}

type TrimEditQueueReply struct {
	Ok bool
}

func (s *ServerRPC) TrimEditQueue(
	req *TrimEditQueueRequest,
	resp *TrimEditQueueReply) error {

	queue, found := s.regionQueueMap[req.Region]
	if found {
		queue.Trim(req.EndSequence)
		resp.Ok = true
	}
	return nil
}
