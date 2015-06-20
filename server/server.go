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
	"net"
	"net/http"
	"net/rpc"
)

const (
	listenPortBase = 8080
)

var (
	rpcPathSequenceNumber int64
)

// Request to add data to pending queue.
type AddDataRequest struct {
	Region balancer.Region
	Data   []byte
}

// Response to AddData request.
type AddDataReply struct {
	Ok bool
}

// Request to get N records from a member of quorum.
type GetNRecordsRequest struct {
	Region          balancer.Region
	StartSequence   int64
	NumberOfRecords int
}

// Response to GetNRecords.
type GetNRecordsReply struct {
	Ok      bool
	Records [][]byte
}

// Trim pending queue to the required sequence number.
type TrimPendingQueueRequest struct {
	Region      balancer.Region
	EndSequence int64
}

type TrimPendingQueueReply struct {
	Ok bool
}

// Request to get the state of a raft member.
type RaftStateRequest struct {
	Region balancer.Region
}

// Response of a raft state query.
type RaftStateReply struct {
	Found bool
	State int
}

type ServerRPC struct {
	regionRaftMap  map[balancer.Region]*RaftStates
	regionQueueMap map[balancer.Region]*PendingQueue
}

func (s *ServerRPC) init() {
	s.regionRaftMap = make(map[balancer.Region]*RaftStates)
	s.regionQueueMap = make(map[balancer.Region]*PendingQueue)
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

func (s *ServerRPC) GetRaftState(req RaftStateRequest, resp *RaftStateReply) error {
	states, found := s.regionRaftMap[req.Region]
	if found {
		states.HandleGetRaftState(req, resp)
	}
	return nil
}

func (s *ServerRPC) AddData(req *AddDataRequest, resp *AddDataReply) error {
	queue, found := s.regionQueueMap[req.Region]
	if found {
		queue.Put(req.Data)
		resp.Ok = true
	}
	return nil
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

func (s *ServerRPC) TrimPendingQueue(
	req *TrimPendingQueueRequest,
	resp *TrimPendingQueueReply) error {

	queue, found := s.regionQueueMap[req.Region]
	if found {
		queue.Trim(req.EndSequence)
		resp.Ok = true
	}
	return nil
}

type Server struct {
	ServerRPC
	impl      *rpc.Server
	port      int
	rpcPath   string
	debugPath string
	listener  net.Listener
}

// In order to support multiple http registrations for different test cases,
// we do not use default RPC path "/rpc". Instead, each test case can have
// a unique prefix string which combined with http server's port number,
// provide a unique http registration for a single test case. This is not
// nencessary in production, though.
func GetServerPath(prefix string, port int) (rpcPath, debugPath string) {
	rpcPath = fmt.Sprintf("/rpc_%s_%d", prefix, port)
	debugPath = fmt.Sprintf("/debug_%s_%d", prefix, port)
	return
}

func NewServer(prefix string, port int) *Server {
	l, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if e != nil {
		fmt.Println("Fails to listen:", e)
		return nil
	}

	rpcPath, debugPath := GetServerPath(prefix, port)

	s := &Server{
		ServerRPC: ServerRPC{},
		impl:      rpc.NewServer(),
		port:      port,
		rpcPath:   rpcPath,
		debugPath: debugPath,
		listener:  l,
	}

	s.ServerRPC.init()

	s.impl.Register(&s.ServerRPC)
	s.impl.HandleHTTP(s.rpcPath, s.debugPath)

	go http.Serve(l, nil)

	return s
}

// Find an available port, return a new server and the associated
// port number.
func NewServerAndPort(prefix string) (s *Server, port int) {
	for i := listenPortBase; i < listenPortBase+1000; i++ {
		s = NewServer(prefix, i)
		if s != nil {
			port = i
			return
		}
	}
	return
}

func (s *Server) Close() {
	s.listener.Close()
}

func (s *Server) GetRpcPath() string {
	return s.rpcPath
}

func (s *Server) GetDebugPath() string {
	return s.debugPath
}

func (s *Server) RegisterRegion(
	r balancer.Region,
	states *RaftStates,
	queue *PendingQueue) {
	s.regionRaftMap[r] = states
	s.regionQueueMap[r] = queue
}

func (s *Server) UnregisterRegion(r balancer.Region) {
	{
		val, found := s.regionRaftMap[r]
		if found {
			val.Close()
			delete(s.regionRaftMap, r)
		}
	}

	{
		val, found := s.regionQueueMap[r]
		if found {
			val.Close()
			delete(s.regionQueueMap, r)
		}
	}
}

func (s *Server) GetPort() int {
	return s.port
}
