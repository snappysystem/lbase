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

import(
	"os"
	"sort"
	"testing"
)

func TestDbCreateSuccess(t *testing.T) {
	path := "/tmp/testDbCreateSuccess"
	os.RemoveAll(path)

	opts := NewDbOptions()
	opts.SetCreateIfMissing(1)

	_,openError := OpenDb(opts, path)
	if openError != nil {
		t.Error("Fails to open db")
	}
}

func TestDbCreateFailure(t *testing.T) {
	path := "/tmp/testDbCreateFailure"
	os.RemoveAll(path)

	opts := NewDbOptions()
	opts.SetCreateIfMissing(0)

	_,openError := OpenDb(opts, path)
	if openError == nil {
		t.Error("Expect to fails to open db")
	}
}

func TestDbPutGet(t *testing.T) {
	path := "/tmp/testDbPutGet"
	os.RemoveAll(path)

	opts := NewDbOptions()
	opts.SetCreateIfMissing(1)

	db,openError := OpenDb(opts, path)
	if openError != nil {
		t.Error("Fails to open db")
	}

	defer db.Close()

	key, value := "hello", "world"

	putError := db.Put(NewWriteOptions(), []byte(key), []byte(value))
	if putError != nil {
		t.Error("Fails to put")
	}

	res,getError := db.Get(NewReadOptions(), []byte(key))
	if getError != nil || string(res) != value {
		t.Error("Fails to get")
	}
}

func TestDbWriteIter(t *testing.T) {
	path := "/tmp/testDbWriteIter"
	os.RemoveAll(path)

	opts := NewDbOptions()
	opts.SetCreateIfMissing(1)

	db,openError := OpenDb(opts, path)
	if openError != nil {
		t.Error("Fails to open db")
	}

	defer db.Close()

	keys := []string{"hello", "world", "goleveldb"}
	batch := NewWriteBatch()
	for _,k := range keys {
		b := []byte(k)
		batch.Put(b, b)
	}

	writeError := db.Write(NewWriteOptions(), batch)
	if writeError != nil {
		t.Error("Fails to multi-write")
	}

	iter := db.CreateIterator(NewReadOptions())
	defer iter.Destroy()

	sort.Strings(keys)

	iter.SeekToFirst()
	if !iter.Valid() || string(iter.Key()) != keys[0] {
		t.Error("Fails to seek to first")
	}

	iter.SeekToLast()
	if !iter.Valid() || string(iter.Key()) != keys[len(keys)-1] {
		t.Error("Fails to seek to first")
	}
}

func TestDbIterValid(t *testing.T) {
	path := "/tmp/testDbIterValid"
	os.RemoveAll(path)

	opts := NewDbOptions()
	opts.SetCreateIfMissing(1)

	db,openError := OpenDb(opts, path)
	if openError != nil {
		t.Error("Fails to open db")
	}

	defer db.Close()

	keys := []string{"hello", "world", "goleveldb"}
	batch := NewWriteBatch()
	for _,k := range keys {
		b := []byte(k)
		batch.Put(b, b)
	}

	writeError := db.Write(NewWriteOptions(), batch)
	if writeError != nil {
		t.Error("Fails to multi-write")
	}

	iter := db.CreateIterator(NewReadOptions())
	defer iter.Destroy()

	iter.Seek([]byte("zebra"))
	if iter.Valid() {
		t.Error("Expect seek to be invalid!")
	}

	iter.Seek([]byte("alpha"))
	if !iter.Valid() || string(iter.Key()) != "goleveldb" {
		t.Error("Expect seek to first element!")
	}
}

func TestDbWriteIterViewChange(t *testing.T) {
	path := "/tmp/testDbWriteIter"
	os.RemoveAll(path)

	opts := NewDbOptions()
	opts.SetCreateIfMissing(1)

	db,openError := OpenDb(opts, path)
	if openError != nil {
		t.Error("Fails to open db")
	}

	defer db.Close()

	keys := []string{"hello", "world", "goleveldb"}
	batch := NewWriteBatch()
	for _,k := range keys {
		b := []byte(k)
		batch.Put(b, b)
	}

	writeError := db.Write(NewWriteOptions(), batch)
	if writeError != nil {
		t.Error("Fails to multi-write")
	}

	iter := db.CreateIterator(NewReadOptions())
	defer iter.Destroy()

	sort.Strings(keys)

	iter.SeekToLast()
	if !iter.Valid() || string(iter.Key()) != keys[len(keys)-1] {
		t.Error("Fails to seek to last")
	}

	last := "zebra"
	db.Put(NewWriteOptions(), []byte(last), []byte(last))

	iter.SeekToLast()
	if !iter.Valid() || string(iter.Key()) != keys[len(keys)-1] {
		t.Error("Fails to seek to last")
	}
}
