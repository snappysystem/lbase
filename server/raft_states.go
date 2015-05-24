package server

import (
	"fmt"
	"lbase/balancer"
	"net/rpc"
	"time"
)

const (
	RAFT_FOLLOWER = iota
	RAFT_CANDIDATE
	RAFT_LEADER
)

type RaftStates struct {
	// Raft state.
	state int
	// Cached value of biggest term.
	lastTerm int64
	opts     *RaftOptions
	// Underlying storage.
	db        *RaftStorage
	clientMap map[balancer.ServerName]*rpc.Client
}

func NewRaftStates(opts *RaftOptions, db *RaftStorage) *RaftStates {
	return &RaftStates{
		state:     RAFT_FOLLOWER,
		opts:      opts,
		db:        db,
		clientMap: make(map[balancer.ServerName]*rpc.Client),
	}
}

func (s *RaftStates) GetLastTerm() int64 {
	if s.lastTerm != 0 {
		return s.lastTerm
	}

	seq := s.db.GetRaftSequence()
	s.lastTerm = seq.Term
	return s.lastTerm
}

func (s *RaftStates) GetClient(name balancer.ServerName) *rpc.Client {
	cli, found := s.clientMap[name]
	if found {
		return cli
	}

	// Only create clients that are in the quorum group.
	isMember := false
	for _, sn := range s.opts.Members {
		if sn == name {
			isMember = true
			break
		}
	}
	if !isMember {
		return nil
	}

	cli, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", name.Host, name.Port))
	if err != nil {
		fmt.Printf("Fails to create connection to %#v: %#v\n", name, err)
		return nil
	}

	return cli
}

func (s *RaftStates) TransitToCandidate() {
	if s.state != RAFT_FOLLOWER {
		panic(fmt.Sprintf("current state is not follower: %#v", s.state))
	}

	s.state = RAFT_CANDIDATE
	go s.CandidateLoop()
}

func (s *RaftStates) CandidateLoop() {
	numMembers := len(s.opts.Members)
	term := s.GetLastTerm() + 1

	for {
		if s.state != RAFT_CANDIDATE {
			return
		}

		cliMap := make(map[balancer.ServerName]*rpc.Client)
		for _, sn := range s.opts.Members {
			cli := s.GetClient(sn)
			if cli != nil {
				cliMap[sn] = cli
			}
		}

		if len(cliMap) <= numMembers/2 {
			time.Sleep(time.Duration(s.opts.CandidateWaitMs) * time.Millisecond)
			continue
		}

		// Update term number if we are falling behind.
		{
			newTerm := s.GetLastTerm()
			if newTerm >= term {
				term = newTerm + 1
			}
		}

		req := RequestVote{
			Region:       s.opts.Region,
			ServerName:   s.opts.Address,
			Term:         term,
			LastSequence: s.db.GetRaftSequence(),
		}

		var calls []*rpc.Call
		for _, cli := range cliMap {
			resp := &RequestVoteReply{}
			call := cli.Go(
				"ServerService.RequestVote",
				&req,
				resp,
				nil)

			calls = append(calls, call)
		}

		callMap := NewMulticast(calls, s.opts.RequestVoteTimeoutMs).WaitAll()

		agreed := 0
		for c, _ := range callMap {
			if c.Reply.(*RequestVoteReply).Ok {
				agreed++
			}
		}

		if agreed > numMembers/2 {
			s.TransitToLeader()
		}

		term++
	}
}

func (s *RaftStates) TransitToLeader() {
	if s.state != RAFT_CANDIDATE {
		return
	}
	s.state = RAFT_LEADER
	go s.LeaderLoop()
}

func (s *RaftStates) LeaderLoop() {
	// Initially assume each member has caught up with the leader.
	progMap := make(map[balancer.ServerName]RaftSequence)
	seq := s.db.GetRaftSequence()
	for _, sn := range s.opts.Members {
		progMap[sn] = seq
	}

	for {
		if s.state != RAFT_LEADER {
			return
		}
	}
}

func (s *RaftStates) HandleRequestVote() {
}
