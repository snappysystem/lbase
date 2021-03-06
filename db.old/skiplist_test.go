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
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestRandomGeneratorReturn0s(t *testing.T) {
	gen := makeRandomGenerator()
	count := 0

	for i := 0; i < 32; i++ {
		res := gen.get()
		if res == 0 {
			count++
		}
	}

	if count < 24 {
		t.Error("Too little counts ", count)
	}
}

func TestSkiplistPutGetSome(t *testing.T) {
	slist := MakeSkiplist()
	data := [...]string{"hello", "world", "go", "language"}

	for _, s := range data {
		arr := []byte(s)
		slist.Put(arr, arr)
	}

	for _, s := range data {
		arr := []byte(s)
		val, ok := slist.Get(arr)
		if !ok || bytes.Compare(arr, val) != 0 {
			t.Error("Fails to find key ", s)
		}
	}
}

func genRandomBytes() []byte {
	size := 2 + rand.Intn(16)
	ret := make([]byte, size)
	for i := 0; i < size; i++ {
		ret[i] = byte(rand.Intn(25)) + 'a'
	}

	return ret
}

func TestOverridePreviousPut(t *testing.T) {
	slist := MakeSkiplist()
	slist.Put([]byte("hello"), []byte("world"))
	slist.Put([]byte("hello"), []byte("one"))

	val, ok := slist.Get([]byte("hello"))
	if !ok || string(val) != "one" {
		t.Error("Fails to find correct value")
	}
}

func TestSkiplistSingleSeek(t *testing.T) {
	slist := MakeSkiplist()
	slist.Put([]byte("1004"), []byte("world"))

	ro := &ReadOptions{}
	iter := slist.NewIterator(ro)

	iter.Seek([]byte("1002"))
	if !iter.Valid() {
		t.Error("Fails to seek!")
	}
}

func TestZigZagScan(t *testing.T) {
	slist := MakeSkiplist()
	slist.Put([]byte("hello"), []byte("world"))

	ro := &ReadOptions{}
	iter := slist.NewIterator(ro)

	iter.SeekToFirst()
	if !iter.Valid() || string(iter.Key()) != "hello" {
		t.Error("Fails to scan skiplist")
	}

	iter.Prev()
	if iter.Valid() {
		t.Error("skiplist should not be valid!")
	}

	iter.SeekToFirst()
	if !iter.Valid() || string(iter.Key()) != "hello" {
		t.Error("Fails to scan skiplist")
	}
}

func TestSkiplistPutGetMore(t *testing.T) {
	const numElements = 5000
	data := make([][]byte, 0, numElements)
	slist := MakeSkiplist()

	for i := 0; i < numElements; i++ {
		key := genRandomBytes()
		data = append(data, key)
		slist.Put(key, key)
	}

	for i, k := range data {
		val, ok := slist.Get(k)
		if !ok || bytes.Compare(val, k) != 0 {
			t.Error("Fails to find key ", i, " ", string(k))
		}
	}
}

func TestSkiplistPutPerf(t *testing.T) {
	const numElements = 2000

	data := make(map[string][]byte)
	slist := MakeSkiplist()

	mapTime := int64(0)
	skiplistTime := int64(0)

	for i := 0; i < numElements; i++ {
		key := genRandomBytes()
		str := string(key)

		{
			t1 := time.Now()
			data[str] = key
			t2 := time.Now()
			delta := t2.Sub(t1).Nanoseconds()
			mapTime = mapTime + delta
		}

		{
			t1 := time.Now()
			slist.Put(key, key)
			t2 := time.Now()
			delta := t2.Sub(t1).Nanoseconds()
			skiplistTime = skiplistTime + delta
		}
	}

	fmt.Println("map uses ", mapTime/numElements, " nanoseconds per op")
	fmt.Println("ski uses ", skiplistTime/numElements, " nanoseconds per op")
}

func TestSkiplistScanForwardSome(t *testing.T) {
	data := [...]string{"go", "hello", "world", "yellow"}
	slist := MakeSkiplist()

	for _, s := range data {
		bs := []byte(s)
		slist.Put(bs, bs)
	}

	ro := &ReadOptions{}
	iter := slist.NewIterator(ro)
	iter.SeekToFirst()

	for _, s := range data {
		if !iter.Valid() {
			t.Error("Not valid at ", s)
		}
		if bytes.Compare(iter.Key(), []byte(s)) != 0 {
			t.Error("Got string ", string(iter.Key()))
		}
		iter.Next()
	}

	if iter.Valid() {
		t.Error("iter should not be valid at this time")
	}
}

func TestSkiplistScanBackwardSome(t *testing.T) {
	data := [...]string{"yellow", "world", "hello", "go"}
	slist := MakeSkiplist()

	for _, s := range data {
		bs := []byte(s)
		slist.Put(bs, bs)
	}

	ro := &ReadOptions{}
	iter := slist.NewIterator(ro)
	iter.SeekToLast()

	for _, s := range data {
		if !iter.Valid() {
			t.Error("Not valid at ", s)
		}
		if bytes.Compare(iter.Key(), []byte(s)) != 0 {
			t.Error("Got string ", string(iter.Key()))
		}
		iter.Prev()
	}

	if iter.Valid() {
		t.Error("iter should not be valid at this time")
	}
}

func TestSkiplistSeek(t *testing.T) {
	data := [...]string{"yellow", "world", "hello", "go"}
	slist := MakeSkiplist()

	for _, s := range data {
		bs := []byte(s)
		slist.Put(bs, bs)
	}

	ro := &ReadOptions{}
	iter := slist.NewIterator(ro)

	iter.Seek([]byte(data[1]))
	if !iter.Valid() || string(iter.Key()) != data[1] {
		t.Error("Fails to seek to exact location")
	}

	iter.Seek([]byte("gzip"))
	if !iter.Valid() || string(iter.Key()) != "hello" {
		t.Error("Fails to seek to closest location", string(iter.Key()))
	}

	iter.Close()
}

// struct to sort a slice of byte slices
type ByteSliceSorter struct {
	bytesList [][]byte
}

func MakeSortInterface(x [][]byte) sort.Interface {
	return &ByteSliceSorter{x}
}

func (a *ByteSliceSorter) Len() int {
	return len(a.bytesList)
}

func (a *ByteSliceSorter) Less(i, j int) bool {
	return bytes.Compare(a.bytesList[i], a.bytesList[j]) < 0
}

func (a *ByteSliceSorter) Swap(i, j int) {
	tmp := a.bytesList[i]
	a.bytesList[i] = a.bytesList[j]
	a.bytesList[j] = tmp
}

func TestSkiplistScanForwardMore(t *testing.T) {
	const numElements = 1000

	slist := MakeSkiplist()
	data := make([][]byte, 0, numElements)

	for i := 0; i < numElements; i++ {
		key := genRandomBytes()
		data = append(data, key)
		slist.Put(key, key)
	}

	sort.Sort(MakeSortInterface(data))

	ro := &ReadOptions{}
	iter := slist.NewIterator(ro)
	iter.SeekToFirst()

	prev := make([]byte, 0)
	for _, bs := range data {
		if bytes.Compare(prev, bs) == 0 {
			continue
		}

		if !iter.Valid() {
			t.Error("Premature end of iteration")
		}

		if bytes.Compare(iter.Key(), bs) != 0 {
			t.Error("Fails to compare ", string(iter.Key()), " ", string(bs))
		}

		prev = bs
		iter.Next()
	}

	if iter.Valid() {
		t.Error("iter should not be valid at this time")
	}
}

func TestSkiplistScanBackwardMore(t *testing.T) {
	const numElements = 1000

	slist := MakeSkiplist()
	data := make([][]byte, 0, numElements)

	for i := 0; i < numElements; i++ {
		key := genRandomBytes()
		data = append(data, key)
		slist.Put(key, key)
	}

	sort.Sort(MakeSortInterface(data))

	ro := &ReadOptions{}
	iter := slist.NewIterator(ro)
	iter.SeekToLast()

	prev := make([]byte, 0)
	for i := len(data) - 1; i >= 0; i-- {
		bs := data[i]
		if bytes.Compare(prev, bs) == 0 {
			continue
		}

		if !iter.Valid() {
			t.Error("Premature end of iteration")
		}

		if bytes.Compare(iter.Key(), bs) != 0 {
			t.Error("Fails to compare ", string(iter.Key()), " ", string(bs))
		}

		prev = bs
		iter.Prev()
	}

	if iter.Valid() {
		t.Error("iter should not be valid at this time")
	}
}
