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

// This file contains code that is used for raft unit tests.

import (
	"fmt"
	"lbase/balancer"
)

// Given a root directory and a region specification, return a new raft states.
// This function can be used to test a single raft instance.
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
	server, _ = NewServer(root, 0)

	if server != nil && states != nil {
		server.RegisterRegion(reg, states)
	}

	return
}

// Given a root directory, a region specification, and the number of
// quorums to create, return a list of raft states objects.
// This function can be used to test a raft quorum.
func initRaftQuorum(
	root string,
	reg balancer.Region,
	num int,
	recreate bool) (rss []*RaftStates, servers []*Server) {

	// First creates @num servers.
	var ports []int
	for i := 0; i < num; i++ {
		server, port := NewServer(root, 0)
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

		servers[i].RegisterRegion(reg, states)
	}

	// Put state machines into FOLLOWER mode.
	for _, states := range rss {
		states.TransitToFollower()
	}

	return
}
