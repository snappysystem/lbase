package server

import (
	"lbase/balancer"
)

type Server struct {
	regionRaftMap map[balancer.Region]*RaftStates
}

func NewServer() *Server {
	return &Server{
		regionRaftMap: make(map[balancer.Region]*RaftStates),
	}
}

func (s *Server) HandleRequestVote(req *RequestVote, resp *RequestVoteReply) {
	states, found := s.regionRaftMap[req.Region]
	if found {
		states.HandleRequestVote(req, resp)
	}
}

func (s *Server) HandleAppendEntries(req *AppendEntries, resp *AppendEntriesReply) {
	states, found := s.regionRaftMap[req.Region]
	if found {
		states.HandleAppendEntries(req, resp)
	}
}
