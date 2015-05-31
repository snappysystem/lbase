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

type Server struct {
	ServerRPC
	impl      *rpc.Server
	port      int
	rpcPath   string
	debugPath string
	listener  net.Listener
}

func GetServerPath(port int) (rpcPath, debugPath string) {
	rpcPath = fmt.Sprintf("/rpc_%d", port)
	debugPath = fmt.Sprintf("/debug_%d", port)
	return
}

func NewServer(port int) *Server {
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if e != nil {
		fmt.Println("Fails to listen:", e)
		return nil
	}

	rpcPath, debugPath := GetServerPath(port)

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
func NewServerAndPort() (s *Server, port int) {
	for i := listenPortBase; i < listenPortBase+1000; i++ {
		s = NewServer(i)
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
