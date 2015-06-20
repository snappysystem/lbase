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
package db

import (
	"fmt"
	"os"
	"testing"
)

func TestL0Compaction(t *testing.T) {
	path := "/tmp/compactor_test/TestL0Compaction"
	os.RemoveAll(path)
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		t.Error("Fails to create testing dir")
	}

	opt := DbOption{
		path:         path,
		env:          MakeNativeEnv(),
		comp:         ByteOrder(0),
		numTblCache:  8,
		minLogSize:   64,
		maxL0Levels:  4,
		minTableSize: 4 * 1024 * 1024,
	}

	db := MakeDb(opt)
	if db == nil {
		t.Error("Fails to create a DB!")
	}

	bigVal := make([]byte, 64)
	for idx, _ := range bigVal {
		bigVal[idx] = 'a'
	}

	var wopt WriteOptions
	var ropt ReadOptions

	db.Put(wopt, []byte("hello"), []byte("world"))

	for i := 1000; i < 1002; i++ {
		key := fmt.Sprintf("%d", i)
		status, finish := db.PutMore(wopt, []byte(key), bigVal)

		if !status.Ok() {
			t.Error("Fails to put an item!")
		}

		// Wait until L0 compaction completes.
		for v := range finish {
			if v != true {
				t.Error("Does not finish compaction successfully")
			}
		}
	}

	// Verify a particular key is present.
	val, status := db.Get(ropt, []byte("hello"))
	if !status.Ok() || string(val) != "world" {
		t.Error("Fails to get a key")
	}

	// Verify that all keys are present.
	it := db.NewIterator(ropt)
	it.SeekToFirst()

	if !it.Valid() || string(it.Key()) != "1000" {
		t.Error("Fails to get expected key")
	}

	it.Next()

	if !it.Valid() || string(it.Key()) != "1001" {
		t.Error("Fails to get expected key")
	}

	it.Next()

	if !it.Valid() || string(it.Key()) != "hello" {
		t.Error("Fails to get expected key")
	}

	it.Close()
}

func TestMergeCompaction(t *testing.T) {
	path := "/tmp/compactor_test/TestMergeCompaction"
	os.RemoveAll(path)
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		t.Error("Fails to create testing dir")
	}

	opt := DbOption{
		path:         path,
		env:          MakeNativeEnv(),
		comp:         ByteOrder(0),
		numTblCache:  4,
		minLogSize:   64,
		maxL0Levels:  4,
		minTableSize: 128,
	}

	db := MakeDb(opt)
	if db == nil {
		t.Error("Fails to create a DB!")
	}

	bigVal := make([]byte, 64)
	for idx, _ := range bigVal {
		bigVal[idx] = 'a'
	}

	var wopt WriteOptions
	var ropt ReadOptions

	for i := 1000; i < 1100; i++ {
		key := fmt.Sprintf("%d", i)
		status, finish := db.PutMore(wopt, []byte(key), bigVal)

		if !status.Ok() {
			t.Error("Fails to put an item!")
		}

		// Wait until L0 compaction completes.
		for v := range finish {
			if v != true {
				t.Error("Does not finish compaction successfully")
			}
		}
	}

	// Verify that all keys are present.
	it := db.NewIterator(ropt)
	it.SeekToFirst()

	for i := 1000; i < 1100; i++ {
		key := fmt.Sprintf("%d", i)
		if !it.Valid() || string(it.Key()) != key {
			t.Error("Fails to get expected key:", key)
		}

		it.Next()
	}

	it.Close()
}
