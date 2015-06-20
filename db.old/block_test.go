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
	"strconv"
	"testing"
)

func TestCreateAndEnumerateBlock(t *testing.T) {
	// first build the block
	data := make([]byte, 4096)
	builder := MakeBlockBuilder(data)

	for i := 100; i < 105; i++ {
		s := strconv.Itoa(i)
		b := []byte(s)
		builder.Add(b, b)
	}

	block, ok := builder.Finalize()
	if !ok {
		t.Error("Fails to build block")
	}

	// test entries are in the block
	order := ByteOrder(0)
	iter := block.NewIterator(order)

	iter.SeekToFirst()
	if !iter.Valid() {
		t.Error("Fails to find the first element")
	}

	for i := 100; i < 105; i++ {
		s := strconv.Itoa(i)
		b := []byte(s)

		if !iter.Valid() {
			t.Error("Fails to find the element")
		}

		if bytes.Compare(b, iter.Key()) != 0 {
			t.Error("Mismatch ", string(b), " versus ", string(iter.Key()))
		}

		iter.Next()
	}

	if iter.Valid() {
		t.Error("iterator pass the end")
	}
}

func TestBlockRandomSeek(t *testing.T) {
	// first build the block
	data := make([]byte, 4096)
	builder := MakeBlockBuilder(data)

	for i := 100; i < 105; i++ {
		s := strconv.Itoa(i)
		b := []byte(s)
		builder.Add(b, b)
	}

	block, ok := builder.Finalize()
	if !ok {
		t.Error("Fails to build block")
	}

	// seek to one entry in the block
	order := ByteOrder(0)
	iter := block.NewIterator(order)
	iter.Seek([]byte("103"))

	for i := 103; i < 105; i++ {
		if !iter.Valid() {
			t.Error("iter ends prematurely")
		}
		s := strconv.Itoa(i)
		if string(iter.Key()) != s {
			t.Error("Fails to seek to ", s)
		}

		iter.Next()
	}

	if iter.Valid() {
		t.Error("iter has extra value")
	}
}

func TestBlockBackwardSeek(t *testing.T) {
	// first build the block
	data := make([]byte, 4096)
	builder := MakeBlockBuilder(data)

	for i := 100; i < 105; i++ {
		s := strconv.Itoa(i)
		b := []byte(s)
		builder.Add(b, b)
	}

	block, ok := builder.Finalize()
	if !ok {
		t.Error("Fails to build block")
	}

	// seek to one entry in the block
	order := ByteOrder(0)
	iter := block.NewIterator(order)
	iter.Seek([]byte("103"))

	for i := 103; i >= 100; i-- {
		if !iter.Valid() {
			t.Error("iter ends prematurely")
		}
		s := strconv.Itoa(i)
		if string(iter.Key()) != s {
			t.Error("Fails to seek to ", s)
		}

		iter.Prev()
	}

	if iter.Valid() {
		t.Error("iter has extra value")
	}
}

func TestBlockEncodeDecode(t *testing.T) {
	// first build the block
	data := make([]byte, 4096)
	var endOffset uint32

	{
		builder := MakeBlockBuilder(data)

		for i := 100; i < 105; i++ {
			s := strconv.Itoa(i)
			b := []byte(s)
			builder.Add(b, b)
		}

		block, ok := builder.Finalize()
		if !ok {
			t.Error("Fails to build block")
		}

		endOffset = uint32(len(block.data))
	}

	{
		block := DecodeBlock(data, endOffset)

		// seek to one entry in the block
		order := ByteOrder(0)
		iter := block.NewIterator(order)
		iter.Seek([]byte("103"))

		for i := 103; i >= 100; i-- {
			if !iter.Valid() {
				t.Error("iter ends prematurely")
			}
			s := strconv.Itoa(i)
			if string(iter.Key()) != s {
				t.Error("Fails to seek to ", s)
			}

			iter.Prev()
		}

		if iter.Valid() {
			t.Error("iter has extra value")
		}
	}
}

func TestCreateAndEnumerateMultiBlock(t *testing.T) {
	data := make([]byte, 4096)
	builder := MakeBlockBuilder(data)
	offset := uint32(0)

	// build first block
	{
		for i := 100; i < 105; i++ {
			s := strconv.Itoa(i)
			b := []byte(s)
			builder.Add(b, b)
		}

		block, ok := builder.Finalize()
		if !ok {
			t.Error("Fails to build block")
		}

		offset = offset + uint32(len(block.data))
	}

	// build second block
	{
		for i := 200; i < 205; i++ {
			s := strconv.Itoa(i)
			b := []byte(s)
			builder.Add(b, b)
		}

		block, ok := builder.Finalize()
		if !ok {
			t.Error("Fails to build block")
		}

		offset = offset + uint32(len(block.data))
	}

	order := ByteOrder(0)

	// test entries are in last block
	{
		block := DecodeBlock(data, offset)
		iter := block.NewIterator(order)

		iter.SeekToFirst()
		if !iter.Valid() {
			t.Error("Fails to find the first element")
		}

		for i := 200; i < 205; i++ {
			s := strconv.Itoa(i)
			b := []byte(s)

			if !iter.Valid() {
				t.Error("Fails to find the element")
			}

			if bytes.Compare(b, iter.Key()) != 0 {
				t.Error("Mismatch ", string(b), " versus ", string(iter.Key()))
			}

			iter.Next()
		}

		if iter.Valid() {
			t.Error("iterator pass the end")
		}

		offset = offset - uint32(len(block.data))
	}

	// test entries are in first block
	{
		block := DecodeBlock(data, offset)
		iter := block.NewIterator(order)

		iter.SeekToFirst()
		if !iter.Valid() {
			t.Error("Fails to find the first element")
		}

		for i := 100; i < 105; i++ {
			s := strconv.Itoa(i)
			b := []byte(s)

			if !iter.Valid() {
				t.Error("Fails to find the element")
			}

			if bytes.Compare(b, iter.Key()) != 0 {
				t.Error("Mismatch ", string(b), " versus ", string(iter.Key()))
			}

			iter.Next()
		}

		if iter.Valid() {
			t.Error("iterator pass the end")
		}
	}
}
