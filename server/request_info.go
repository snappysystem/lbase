package server

import (
	"net/rpc"
)

// Common data structure needed for a RPC call.
type RequestInfo struct {
	Cli  *rpc.Client
	Call *rpc.Call
}
