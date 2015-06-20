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
	"log"
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
	// If this is the leader, hold current term value. Otherwise, it is 0.
	leaderTerm int64
	opts       *RaftOptions
	// Underlying storage.
	db        *RaftStorage
	clientMap map[balancer.ServerName][]*rpc.Client
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
		clientMap:          make(map[balancer.ServerName][]*rpc.Client),
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
	cls, found := s.clientMap[name]
	if found && len(cls) > 0 {
		lastIdx := len(cls) - 1
		ret := cls[lastIdx]
		cls = cls[:lastIdx]
		s.clientMap[name] = cls
		return ret
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

	addr := fmt.Sprintf("%s:%d", name.Host, name.Port)
	path, _ := GetServerPath(s.opts.RPCPrefix, name.Port)

	cli, err := rpc.DialHTTPPath("tcp", addr, path)
	if err != nil {
		log.Printf("Fails to create connection to %s: %#v\n", addr, err)
		return nil
	}

	return cli
}

func (s *RaftStates) ReturnClient(name balancer.ServerName, cli *rpc.Client) {
	cls, _ := s.clientMap[name]
	if len(cls) > 4 {
		return
	}
	cls = append(cls, cli)
	s.clientMap[name] = cls
}

func (s *RaftStates) TransitToCandidate() {
	if s.state != RAFT_FOLLOWER {
		log.Panic("current state is not follower: ", s.state)
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

		waitTimeMs := time.Duration(s.opts.CandidateWaitMs)
		waitChan := time.After(waitTimeMs * time.Millisecond)

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

		// Start multicasting with timeout.
		callName := "ServerRPC.RequestVote"

		period := time.Duration(s.opts.RequestVoteTimeoutMs)
		timeCh := time.After(period * time.Millisecond)

		calls := make(map[balancer.ServerName]*rpc.Call)
		for sn, cli := range cliMap {
			resp := &RequestVoteReply{}
			calls[sn] = cli.Go(callName, &req, resp, nil)
		}

		agreed := 0
		for sn, call := range calls {
			select {
			case <-call.Done:
				reply := call.Reply.(*RequestVoteReply)
				if reply.Ok {
					agreed++
				}
				if reply.MyTerm > term {
					term = reply.MyTerm
				}
				cli, found := cliMap[sn]
				if !found {
					log.Panic("Server ", sn, " not found")
				}
				s.ReturnClient(sn, cli)
			case <-timeCh:
				timeCh = time.After(time.Duration(0))
			}
		}

		if agreed > numMembers/2 {
			s.TransitToLeader(term)
			return
		}

		<-waitChan
		term++
	}
}

func (s *RaftStates) TransitToLeader(term int64) {
	if s.state != RAFT_CANDIDATE {
		return
	}
	s.state = RAFT_LEADER
	s.leaderTerm = term
	go s.LeaderLoop(term)
}

func (s *RaftStates) LeaderLoop(term int64) {
	// Remember each server's replication progress.
	progMap := make(map[balancer.ServerName]RaftSequence)
	unknownProgressMap := make(map[balancer.ServerName]bool)
	// TODO: How to get those records?
	pendingRecords := make(map[RaftSequence][]byte)

	callName := "ServerRPC.AppendEntries"
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
		callMap := make(map[balancer.ServerName]RequestInfo)

		for _, sn := range s.opts.Members {
			// If we do not know the progress of a particular server yet,
			// assume that it is already caught up.
			_, found := progMap[sn]
			if !found {
				progMap[sn] = lastSeq
				unknownProgressMap[sn] = true
			}

			// Make a connection to each of servers in the quorum.
			cli := s.GetClient(sn)
			if cli == nil {
				continue
			}

			info := RequestInfo{Cli: cli}

			// Send RPC to each of member servers.
			req := AppendEntries{
				ServerName: s.opts.Address,
				Term:       term,
				Region:     s.opts.Region,
				Data:       make(map[RaftSequence][]byte),
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
					req.Data = s.ScanNoncommitLogs(&progSeq)
				}

				// Appending any new records.
				for key, value := range pendingRecords {
					req.Data[key] = value
				}
			}

			var reply AppendEntriesReply
			info.Call = cli.Go(callName, &req, &reply, nil)
			callMap[sn] = info
		}

		var zeroSequence RaftSequence

		timeOut := time.Duration(s.opts.RaftLeaderTimeoutMs / 2)
		timeChan := time.After(timeOut * time.Millisecond)

		// Processing RPC replies.
		noLongerLeader := false
		for sn, info := range callMap {
			select {
			case <-info.Call.Done:
				resp := info.Call.Reply.(*AppendEntriesReply)
				if resp.NotLeader {
					noLongerLeader = true
					break
				}

				prog, hasProg := progMap[sn]
				if !hasProg {
					panic("Do not have progMap entry!")
				}

				if resp.RealSequence == zeroSequence {
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

				s.ReturnClient(sn, info.Cli)
			case <-timeChan:
				timeChan = time.After(time.Duration(0))
			}
		}

		if noLongerLeader {
			s.TransitToFollower()
			return
		}

		<-timeoutChan
	}
}

func (s *RaftStates) TransitToFollower() {
	s.state = RAFT_FOLLOWER
	if s.leaderTerm != 0 {
		s.leaderTerm = 0
	}
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
		}
	}
}

func (s *RaftStates) ScanNoncommitLogs(start *RaftSequence) map[RaftSequence][]byte {
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

	if s.state == RAFT_CANDIDATE {
		s.TransitToFollower()
	} else if s.state == RAFT_LEADER {
		if req.Term < s.leaderTerm {
			resp.NotLeader = true
			return
		} else if req.Term == s.leaderTerm {
			if req.ServerName != s.opts.Address {
				log.Panic("Having two leaders for the same term!")
			}
		} else {
			s.TransitToFollower()
		}
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

func (s *RaftStates) HandleGetRaftState(req RaftStateRequest, resp *RaftStateReply) {
	resp.Found = true
	resp.State = s.state
}

func (s *RaftStates) TrimTermMap() {
	sureTerm := s.db.GetCommitSequence().Term
	for k, _ := range s.termMap {
		if k < sureTerm {
			delete(s.termMap, k)
		}
	}
}

func (s *RaftStates) Close() {
	s.db.Close()
}

func (s *RaftStates) GetStorage() *RaftStorage {
	return s.db
}

func (s *RaftStates) preparePendingRecords() map[RaftSequence][]byte {
	return nil
}
