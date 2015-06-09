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
	regionRaftMap map[balancer.Region]*RaftStates
}

func (s *ServerRPC) init() {
	s.regionRaftMap = make(map[balancer.Region]*RaftStates)
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

func (s *Server) RegisterRegion(r balancer.Region, states *RaftStates) {
	s.regionRaftMap[r] = states
}

func (s *Server) UnregisterRegion(r balancer.Region) {
	states, found := s.regionRaftMap[r]
	if found {
		states.Close()
		delete(s.regionRaftMap, r)
	}
}

func (s *Server) GetPort() int {
	return s.port
}
