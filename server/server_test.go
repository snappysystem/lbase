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
	"net/rpc"
	"testing"
)

func TestServerAlive(t *testing.T) {
	rpcPathPrefix := "TestServerAlive"
	serv, port := NewServer(rpcPathPrefix, 0)
	if serv == nil {
		t.Error("Fails to create a server!")
	}

	defer serv.Close()

	rpcPath := serv.GetRpcPath()
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	cli, err := rpc.DialHTTPPath("tcp", addr, rpcPath)
	if err != nil {
		t.Error(fmt.Sprintf("Fails to connect: %#v", err))
	}

	req := 7
	var resp int

	rpcerr := cli.Call("ServerRPC.Echo", req, &resp)
	if rpcerr != nil {
		t.Error(fmt.Sprintf("Fails to call: %#v", rpcerr))
	}

	if resp != req {
		t.Error("Result mismatch!")
	}
}

func TestAsyncCall(t *testing.T) {
	rpcPathPrefix := "TestAsyncCall"
	serv, port := NewServer(rpcPathPrefix, 0)
	if serv == nil {
		t.Error("Fails to create a server!")
	}

	defer serv.Close()

	rpcPath := serv.GetRpcPath()
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	cli, err := rpc.DialHTTPPath("tcp", addr, rpcPath)
	if err != nil {
		t.Error(fmt.Sprintf("Fails to connect: %#v", err))
	}

	req := 7
	var resp int

	call := cli.Go("ServerRPC.Echo", req, &resp, nil)
	<-call.Done
	if resp != req {
		t.Error("Result mismatch!")
	}
}
