package zk

/**
This test depends on a running zookeeper instance locally. If you cannot
run a zookeeper instance locally, please disable this test.

How to run zookeeper locally:

(1) Install a zookeeper package;

(2) Create a data dir for zookeeper:

mk -p /tmp/cfg/zk

(3) Create a config file with following text:

tickTime=2000
dataDir=/tmp/cfg/zk
clientPort=18888

(4) Run zookeeper:

./zkServer.sh start <Your zookeeper config file path>
*/

import (
	"testing"
	"fmt"
)

const (
	// Zookeeper port, pls change this to be the real port number
	// on your host.
	Port = 18888
	// Zookeeper timeout value.
	Timeout = 4000
)


func TestZkCreate(t *testing.T) {
	path := "/testZkCreate"
	value := "something"
	service := fmt.Sprintf("localhost:%d", Port)
	h,ok := NewZHandle(service, Timeout, nil)
	if !ok {
		t.Error("Fails to create a zookeeper handle")
	}

	// Make sure that previous leftovers are all gone.
	h.Delete(path, -1)

	// Create a new znode.
	strRes := h.Create(path, value, ZOO_OPEN_ACLS, 0)
	if strRes.GetRc() != ZOK {
		t.Error("Fails to create a path")
	}

	statRes := h.Exists(path)
	if statRes.GetRc() != ZOK {
		t.Error("Fails to call exists()")
	}
}

func TestZkGet(t *testing.T) {
	path := "/testZkGet"
	value := "something"
	service := fmt.Sprintf("localhost:%d", Port)
	h,ok := NewZHandle(service, Timeout, nil)
	if !ok {
		t.Error("Fails to create a zookeeper handle")
	}

	// Make sure that previous leftovers are all gone.
	h.Delete(path, -1)

	// Create a new znode.
	strRes := h.Create(path, value, ZOO_OPEN_ACLS, 0)
	if strRes.GetRc() != ZOK {
		t.Error("Fails to create a path")
	}

	dataRes := h.Get(path)
	if dataRes.GetRc() != ZOK {
		t.Error("Fails to call exists()")
	}

	if value != string(dataRes.GetData()) {
		t.Error("Fails to get the same data!")
	}
}

func TestZkExistW(t *testing.T) {
	path := "/testZkExistW"
	value := "something"
	service := fmt.Sprintf("localhost:%d", Port)
	h,ok := NewZHandle(service, Timeout, nil)
	if !ok {
		t.Error("Fails to create a zookeeper handle")
	}

	// Make sure that previous leftovers are all gone.
	h.Delete(path, -1)

	_,watcher := h.ExistsW(path)

	// Test channel is not available.
	select {
		case _,ok := <-watcher:
			if ok {
				t.Error("Value should not be available yet")
			} else {
				t.Error("Channel should not be closed yet")
			}
		// Expect default behavior
		default:
	}

	// Create a new znode.
	strRes := h.Create(path, value, ZOO_OPEN_ACLS, 0)
	if strRes.GetRc() != ZOK {
		t.Error("Fails to create a path")
	}

	// Test channel is available by now.
	select {
		case event,ok := <-watcher:
			if ok {
				if event.Type != ZOO_CREATED_EVENT {
					t.Error("Expect created event")
				}
				if event.State != ZOO_CONNECTED_STATE {
					t.Error("Expect connected state")
				}
				if event.Path != path {
					t.Error("Path mismatch")
				}
			} else {
				t.Error("Channel should not be closed yet")
			}
		// Expect default behavior
		default:
			t.Error("Channel should be available by now")
	}
}
