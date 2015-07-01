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
	"strconv"
)

var (
	rpcPathSequenceNumber int64
)

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

func NewServer(prefix string, port int) (s *Server, rport int) {
	l, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if e != nil {
		fmt.Println("Fails to listen:", e)
		return
	}

	_, strPort, perr := net.SplitHostPort(l.Addr().String())
	if perr != nil {
		l.Close()
		return
	}

	tmpInt, parseErr := strconv.ParseInt(strPort, 0, 0)
	if parseErr != nil {
		l.Close()
		return
	}

	rport = int(tmpInt)
	rpcPath, debugPath := GetServerPath(prefix, rport)

	s = &Server{
		ServerRPC: ServerRPC{},
		impl:      rpc.NewServer(),
		port:      rport,
		rpcPath:   rpcPath,
		debugPath: debugPath,
		listener:  l,
	}

	s.ServerRPC.init()

	s.impl.Register(&s.ServerRPC)
	s.impl.HandleHTTP(s.rpcPath, s.debugPath)

	go http.Serve(l, nil)

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
	queue *EditQueue) {
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
