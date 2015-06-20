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

func TestSimplePut(t *testing.T) {
	path := "/tmp/dbImpl_test/TestSimplePut"
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
		minLogSize:   4096,
		maxL0Levels:  4,
		minTableSize: 4 * 1024 * 1024,
	}

	db := MakeDb(opt)
	if db == nil {
		t.Error("Fails to create a DB!")
	}

	data := map[string]string{
		"hello":  "world",
		"34567":  "dffafa",
		"others": "27182",
	}

	var wopt WriteOptions
	var ropt ReadOptions

	// Test put.
	for k, v := range data {
		status := db.Put(wopt, []byte(k), []byte(v))
		if !status.Ok() {
			t.Error("Fails to put a key")
		}
	}

	// Test get.
	for k, v := range data {
		val, status := db.Get(ropt, []byte(k))
		if !status.Ok() || string(val) != v {
			t.Error("Fails to get a key")
		}
	}

	// Test iterator.
	it := db.NewIterator(ropt)
	it.SeekToFirst()

	dcopy := make(map[string]string)
	for k, v := range data {
		dcopy[k] = v
	}

	for it.Valid() {
		k := it.Key()
		if _, found := dcopy[string(k)]; found == false {
			t.Error("Fails to find a key ", string(k))
		}

		delete(dcopy, string(k))
		it.Next()
	}

	if len(dcopy) > 0 {
		t.Error("Fails to iterate all elements")
	}
}

func TestQueryForMultipleTables(t *testing.T) {
	path := "/tmp/dbImpl_test/TestQueryForMultipleTables"
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
		maxL0Levels:  8,
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

	channels := make([]chan bool, 0)
	for i := 1; i < 4; i++ {
		s := fmt.Sprintf("%d", i)
		status, finish := db.PutMore(wopt, []byte(s), bigVal)
		if !status.Ok() {
			t.Error("Fails to put an item!")
		}

		if finish != nil {
			channels = append(channels, finish)
		}
	}

	// Wait until all compactions finish.
	for _, c := range channels {
		for v := range c {
			if v != true {
				t.Error("compactor should return true!")
			}
		}
	}

	// Confirm that all entries are still there.
	for i := 1; i < 4; i++ {
		val, status := db.Get(ropt, []byte(fmt.Sprintf("%d", i)))
		if !status.Ok() || string(val) != string(bigVal) {
			t.Error("Fails to get a key")
		}
	}
}
