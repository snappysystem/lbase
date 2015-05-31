package server

import (
	"fmt"
	"net/rpc"
	"testing"
)

func TestServerAlive(t *testing.T) {
	serv, port := NewServerAndPort()
	if serv == nil {
		t.Error("Fails to create a server!")
	}

	defer serv.Close()

	rpcPath, _ := GetServerPath(port)
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
