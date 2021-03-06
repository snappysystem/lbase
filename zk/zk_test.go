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

	if statRes.GetVersion() != 0 {
		t.Error("Expected version ", statRes.GetVersion())
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

	// Verify that data has been written correctly.
	dataRes := h.Get(path)
	if dataRes.GetRc() != ZOK {
		t.Error("Fails to call exists()")
	}

	if value != string(dataRes.GetData()) {
		t.Error("Fails to get the same data!")
	}
}

func TestZkSet(t *testing.T) {
	path := "/testZkSet"
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

	newValue := "newValue"
	statRes := h.Set(path, []byte(newValue), 0)

	if statRes.GetRc() != ZOK {
		t.Error("Fails to set a value")
	}
	if statRes.GetVersion() != 1 {
		t.Error("Real version number is ", statRes.GetVersion())
	}
}

func TestZkGetChildren(t *testing.T) {
	path := "/testZkGetChildren"
	value := "something"
	service := fmt.Sprintf("localhost:%d", Port)
	h,ok := NewZHandle(service, Timeout, nil)
	if !ok {
		t.Error("Fails to create a zookeeper handle")
	}

	// Make sure that previous leftovers are all gone.
	h.Delete(path + "/a", -1)
	h.Delete(path + "/b", -1)
	h.Delete(path, -1)

	// Create new znodes.
	strRes := h.Create(path, value, ZOO_OPEN_ACLS, 0)
	if strRes.GetRc() != ZOK {
		t.Error("Fails to create a path")
	}

	strRes = h.Create(path + "/a", value, ZOO_OPEN_ACLS, 0)
	if strRes.GetRc() != ZOK {
		t.Error("Fails to create a path")
	}

	strRes = h.Create(path + "/b", value, ZOO_OPEN_ACLS, 0)
	if strRes.GetRc() != ZOK {
		t.Error("Fails to create a path")
	}

	// Verify GetChildren() is correct.
	stringsRes := h.GetChildren(path)
	if stringsRes.GetRc() != ZOK {
		t.Error("Fails to call GetChildren")
	}
	strings := stringsRes.GetStrings()
	if len(strings) != 2 {
		t.Error("Has number of strings: ", len(strings))
	}
}
