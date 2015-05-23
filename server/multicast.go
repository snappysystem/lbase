package server

import (
	"net/rpc"
	"time"
)

func Multicast(calls []*rpc.Call, timeOutMs int64) map[*rpc.Call]bool {
	var ctrlChans []chan bool
	doneChan := make(chan *rpc.Call, len(calls))

	for _, call := range calls {
		if call != nil {
			exitChan := make(chan bool, 1)
			ctrlChans = append(ctrlChans, exitChan)

			go func() {
				shouldExit := true
				for {
					select {
					case <-exitChan:
					case <-call.Done:
						doneChan <- call
					default:
						shouldExit = false
					}
					if shouldExit {
						break
					}
				}
			}()
		}
	}

	// Process replies.
	replies := make(map[*rpc.Call]bool)
	for len(replies) < len(ctrlChans) {
		start := time.Now()
		needsBreak := false

		select {
		case call := <-doneChan:
			replies[call] = true
		case <-time.After(time.Duration(timeOutMs) * time.Millisecond):
			needsBreak = true
		default:
		}

		if needsBreak {
			break
		}

		dur := time.Since(start)
		timeOutMs = timeOutMs - dur.Nanoseconds()*1000000
		if timeOutMs <= 0 {
			break
		}
	}

	// Signal to quit all pending RPC go routines.
	for _, ch := range ctrlChans {
		ch <- true
	}

	return replies
}
