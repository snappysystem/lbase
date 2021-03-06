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
	"testing"
	"time"
)

func TestRaftStatesInitialized(t *testing.T) {
	log.SetFlags(log.Lshortfile)

	root := "/tmp/TestRaftStatesInitialized"
	reg := balancer.Region{}

	states, server := initRaftStates(root, reg, true)
	if server == nil {
		t.Error("Fails to create a server!")
	}

	defer server.Close()

	if states == nil {
		t.Error("Fails to create a raft states!")
	}
}

func TestRaftLeaderElection(t *testing.T) {
	log.SetFlags(log.Lshortfile)

	root := "/tmp/TestRaftLeaderElection"
	reg := balancer.Region{}
	num := 3

	// Create a quorum of three members.
	rss, servers := initRaftQuorum(root, reg, num, true)
	defer func() {
		for _, serv := range servers {
			serv.Close()
		}
	}()

	for j := 0; j < 5; j++ {
		results := make(map[int]int)
		time.Sleep(time.Second)
		for i := 0; i < num; i++ {
			if rss[i] == nil || servers[i] == nil {
				t.Error("Fails to create quorum")
			}

			r := RaftStateRequest{Region: reg}
			var reply RaftStateReply

			// Creating client that connects to the server.
			rpcPath := servers[i].GetRpcPath()
			addr := fmt.Sprintf("127.0.0.1:%d", servers[i].GetPort())

			cli, err := rpc.DialHTTPPath("tcp", addr, rpcPath)
			if err != nil {
				log.Fatal("Fails to connect: ", err)
			}

			err = cli.Call("ServerRPC.GetRaftState", r, &reply)
			if err != nil {
				log.Fatal("Fails to do RPC call: ", err)
			}

			if reply.Found {
				count, _ := results[reply.State]
				count++
				results[reply.State] = count
			}
		}

		leaderCnt, _ := results[RAFT_LEADER]
		followerCnt, _ := results[RAFT_FOLLOWER]

		if leaderCnt == 1 && followerCnt == 2 {
			return
		} else if leaderCnt > 1 {
			t.Error("Having more than one leaders!")
		}
	}

	t.Error("Fails to elect a leader in given time!")
}
