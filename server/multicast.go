package server

import (
	"net/rpc"
	"time"
)

type Multicast struct {
	ctrlChans   []chan bool
	doneChan    chan *rpc.Call
	timeoutChan chan bool
}

func NewMulticast(calls []*rpc.Call, timeoutMs int64) *Multicast {
	ret := &Multicast{
		ctrlChans:   make([]chan bool, 1),
		doneChan:    make(chan *rpc.Call, len(calls)),
		timeoutChan: make(chan bool, 1),
	}

	// Setup timeout channel.
	go func() {
		<-time.After(time.Duration(timeoutMs) * time.Millisecond)
		ret.timeoutChan <- true
	}()

	for _, call := range calls {
		if call != nil {
			exitChan := make(chan bool, 1)
			ret.ctrlChans = append(ret.ctrlChans, exitChan)

			go func() {
				for {
					select {
					case <-exitChan:
					case <-call.Done:
						ret.doneChan <- call
					}
					break
				}
			}()
		}
	}

	return ret
}

// Clean up resources: quit all pending go rotines.
func (m *Multicast) Close() {
	// Signal to quit all pending RPC go routines.
	for _, ch := range m.ctrlChans {
		ch <- true
	}
}

// Wait for all rpc finish, or timeout occurs, then return
// all available replies. The call also close the Multicast
// operation automatically.
func (m *Multicast) WaitAll() map[*rpc.Call]bool {
	// Process replies.
	replies := make(map[*rpc.Call]bool)
	for len(replies) < len(m.ctrlChans) {
		needsBreak := false
		select {
		case call := <-m.doneChan:
			replies[call] = true
		case <-m.timeoutChan:
			needsBreak = true
		default:
		}

		if needsBreak {
			break
		}
	}

	m.Close()

	return replies
}

// Wait for one pending rpc to finish. If there is no pending rpc or
// timeout has reached, return nil. The caller is responsible to
// close this Multicast object after it is no longer needed.
func (m *Multicast) WaitOne() *rpc.Call {
	for {
		select {
		case call := <-m.doneChan:
			return call
		case <-m.timeoutChan:
			return nil
		default:
		}
	}

	return nil
}
