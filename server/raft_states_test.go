package server

import (
	"fmt"
	"lbase/balancer"
	"log"
	"net/rpc"
	"testing"
	"time"
)

func initRaftStates(
	root string,
	reg balancer.Region,
	recreate bool) (states *RaftStates, server *Server) {

	store := initRaftStorageForTest(root, reg, recreate)
	if store == nil {
		return
	}

	opts := store.GetRaftOptions()
	states = NewRaftStates(opts, store)
	server, _ = NewServerAndPort(root)

	if server != nil && states != nil {
		server.RegisterRegion(reg, states, nil)
	}

	return
}

func initRaftQuorum(
	root string,
	reg balancer.Region,
	num int,
	recreate bool) (rss []*RaftStates, servers []*Server) {

	// First creates @num servers.
	var ports []int
	for i := 0; i < num; i++ {
		server, port := NewServerAndPort(root)
		if server == nil {
			panic("Fails to create a server")
		}

		servers = append(servers, server)
		ports = append(ports, port)
	}

	// Figure out all members of this quorum
	var names []balancer.ServerName
	for i := 0; i < num; i++ {
		sn := balancer.ServerName{Host: "127.0.0.1", Port: ports[i]}
		names = append(names, sn)
	}

	// Create raft states objects.
	for i := 0; i < num; i++ {
		name := fmt.Sprintf("%s/%d", root, i)
		store := initRaftStorageForTest(name, reg, recreate)
		if store == nil {
			panic("Fails to create a store")
		}

		opts := store.GetRaftOptions()
		opts.Address = balancer.ServerName{Host: "127.0.0.1", Port: ports[i]}
		opts.Members = names
		opts.RPCPrefix = root

		states := NewRaftStates(opts, store)
		rss = append(rss, states)

		servers[i].RegisterRegion(reg, states, nil)
	}

	// Put state machines into FOLLOWER mode.
	for _, states := range rss {
		states.TransitToFollower()
	}

	return
}

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
