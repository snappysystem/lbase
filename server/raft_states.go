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
	// A channel to receive leader's activity.
	leaderActivityChan chan bool
	// Maps voting history for each of term.
	termMap map[int64]balancer.ServerName
}

func NewRaftStates(opts *RaftOptions, db *RaftStorage) *RaftStates {
	return &RaftStates{
		state:              RAFT_FOLLOWER,
		opts:               opts,
		db:                 db,
		clientMap:          make(map[balancer.ServerName]*rpc.Client),
		leaderActivityChan: make(chan bool, 1024),
		termMap:            make(map[int64]balancer.ServerName),
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
			reply := c.Reply.(*RequestVoteReply)
			if reply.Ok {
				agreed++
			}
			if reply.MyTerm > term {
				term = reply.MyTerm
			}
		}

		if agreed > numMembers/2 {
			s.TransitToLeader(term)
		}

		term++
	}
}

func (s *RaftStates) TransitToLeader(term int64) {
	if s.state != RAFT_CANDIDATE {
		return
	}
	s.state = RAFT_LEADER
	go s.LeaderLoop(term)
}

func (s *RaftStates) LeaderLoop(term int64) {
	// Remember each server's replication progress.
	progMap := make(map[balancer.ServerName]RaftSequence)
	unknownProgressMap := make(map[balancer.ServerName]bool)
	// TODO: How to get those records?
	pendingRecords := make(map[RaftSequence][]byte)

	for {
		if s.state != RAFT_LEADER {
			return
		}

		// Set a timer so that leader loop will not progress too fast.
		timeoutChan := make(chan bool, 1)
		go func() {
			ms := s.opts.RaftLeaderTimeoutMs * 3 / 4
			time.Sleep(time.Duration(ms) * time.Millisecond)
			timeoutChan <- true
		}()

		lastSeq := s.db.GetRaftSequence()
		cliMap := make(map[balancer.ServerName]*rpc.Client)

		for _, sn := range s.opts.Members {
			// Make a connection to each of servers in the quorum.
			cli := s.GetClient(sn)
			if cli != nil {
				cliMap[sn] = cli
			}

			// If we do not know the progress of a particular server yet,
			// assume that it is already caught up.
			_, found := progMap[sn]
			if !found {
				progMap[sn] = lastSeq
				unknownProgressMap[sn] = true
			}
		}

		callMap := make(map[*rpc.Call]balancer.ServerName)

		// Send RPC to each of member servers.
		for sn, cli := range cliMap {
			req := AppendEntries{
				Term:   term,
				Region: s.opts.Region,
				Data:   make(map[RaftSequence][]byte),
			}

			progSeq, hasProgSeq := progMap[sn]
			if !hasProgSeq {
				panic("Fails to find progress")
			}

			_, unknownProgress := unknownProgressMap[sn]
			if unknownProgress {
				req.LeaderGuessedSequence = progSeq
			} else if progSeq.Index > 0 {
				// Get data from raft logs.
				if progSeq.Less(lastSeq) {
					req.Data = s.ScanPendingLogs(&progSeq)
				}

				// Appending any new records.
				for key, value := range pendingRecords {
					req.Data[key] = value
				}
			}

			var reply AppendEntriesReply
			call := cli.Go(
				"Server.AppendEntries",
				&req,
				&reply,
				nil)

			callMap[call] = sn
		}

		var calls []*rpc.Call
		for c, _ := range callMap {
			calls = append(calls, c)
		}

		mcast := NewMulticast(calls, s.opts.RaftLeaderTimeoutMs/2)
		noLongerLeader := false

		// Check for AppendEntries replies.
		for {
			call := mcast.WaitOne()
			if call == nil {
				break
			}

			resp := call.Reply.(*AppendEntriesReply)
			if resp.NotLeader {
				noLongerLeader = true
				break
			}

			sn, hasSn := callMap[call]
			if !hasSn {
				panic("Do not have server name!")
			}

			prog, hasProg := progMap[sn]
			if !hasProg {
				panic("Do not have progMap entry!")
			}

			if resp.RealSequence.Term == 0 && resp.RealSequence.Index == 0 {
				// If the client want to be left alone, do so.
				delete(unknownProgressMap, sn)
				progMap[sn] = resp.RealSequence
			} else if resp.RealSequence == prog {
				// Leader guessed correctly, enable record replication.
				delete(unknownProgressMap, sn)
			} else if prog.Less(resp.RealSequence) {
				// Replication made progress.
				progMap[sn] = resp.RealSequence
			} else {
				panic("real sequence is less than previous progress!")
			}
		}

		mcast.Close()

		if noLongerLeader {
			s.TransitToFollower()
			return
		}

		<-timeoutChan
	}
}

func (s *RaftStates) TransitToFollower() {
	s.state = RAFT_FOLLOWER
	go s.FollowerLoop()
}

func (s *RaftStates) FollowerLoop() {
	ms := s.opts.RaftLeaderTimeoutMs
	for {
		select {
		case <-s.leaderActivityChan:
		case <-time.After(time.Duration(ms) * time.Millisecond):
			s.TransitToCandidate()
			return
		default:
		}
	}
}

func (s *RaftStates) ScanPendingLogs(start *RaftSequence) map[RaftSequence][]byte {
	iter := s.db.log.CreateIterator(s.db.rdOpts)
	defer iter.Destroy()

	iter.Seek(start.AsKey())
	ret := make(map[RaftSequence][]byte)

	cur := start.Index

	for iter.Valid() {
		newSeq, keyError := NewRaftSequenceFromKey(iter.Key())
		if keyError != nil {
			panic("Bad keys in log!")
		}
		if cur == newSeq.Index {
			ret[*newSeq] = iter.Value()
			cur++

		} else {
			break
		}
	}

	return ret
}

func (s *RaftStates) HandleRequestVote(req *RequestVote, resp *RequestVoteReply) {
	lastTerm := s.GetLastTerm()
	if req.Term <= lastTerm {
		resp.Ok = false
	} else if req.LastSequence.Less(s.db.GetRaftSequence()) {
		resp.Ok = false
	} else {
		sn, hasVote := s.termMap[req.Term]
		if !hasVote {
			s.termMap[req.Term] = req.ServerName
			resp.Ok = true

			if len(s.termMap) > 10 {
				go s.TrimTermMap()
			}
		} else if sn != req.ServerName {
			resp.Ok = false
		} else {
			resp.Ok = true
		}
	}

	if !resp.Ok && req.Term < lastTerm {
		resp.MyTerm = lastTerm
	}
}

func (s *RaftStates) HandleAppendEntries(req *AppendEntries, resp *AppendEntriesReply) {
	if req.Term < s.GetLastTerm() {
		resp.NotLeader = true
		return
	}

	s.leaderActivityChan <- true

	// If leader has given up, do not check the data with the request.
	var zeroSequence RaftSequence
	if req.LeaderGuessedSequence == zeroSequence {
		return
	}

	// If leader has not figure out my progress yet, give it a hint.
	iter := s.db.log.CreateIterator(s.db.rdOpts)
	defer iter.Destroy()

	var savedSeq RaftSequence
	var matchedSeq bool

	iter.Seek(req.LeaderGuessedSequence.AsKey())
	if iter.Valid() {
		tmp, _ := NewRaftSequenceFromKey(iter.Key())
		savedSeq = *tmp
		if savedSeq == req.LeaderGuessedSequence {
			matchedSeq = true
		}
	}

	// If the sequence has not been matched, try to find the previous
	// sequence number.
	if !matchedSeq {
		if iter.Valid() {
			iter.Prev()
			if iter.Valid() {
				tmp, _ := NewRaftSequenceFromKey(iter.Key())
				resp.RealSequence = *tmp
			}
		}

		// If we cannot move backward, we should let leader to give up
		// by setting resp.RealSequence to 0, which is the default
		// behavior.
	}
}

func (s *RaftStates) TrimTermMap() {
	sureTerm := s.db.GetCommitSequence().Term
	for k, _ := range s.termMap {
		if k < sureTerm {
			delete(s.termMap, k)
		}
	}
}
