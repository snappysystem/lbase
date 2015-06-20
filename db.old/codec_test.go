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
	"testing"
)

func TestEncodeAndDecodeUint32(t *testing.T) {
	scratch := make([]byte, 0)
	data := [...]uint32{2345, 34, 123456, 345}

	for _, val := range data {
		scratch = EncodeUint32(scratch, val)
	}

	for _, val := range data {
		var res uint32
		oldSize := len(scratch)
		res, scratch = DecodeUint32(scratch)

		if oldSize == len(scratch) {
			t.Error("Fails to decode")
		}

		if res != val {
			t.Error("decode to a wrong value ", res)
		}
	}
}

func TestEncodeAndDecodeUint64(t *testing.T) {
	scratch := make([]byte, 0)
	data := [...]uint64{876501232345, 34, 123456, 345}

	for _, val := range data {
		scratch = EncodeUint64(scratch, val)
	}

	for _, val := range data {
		var res uint64
		oldSize := len(scratch)
		res, scratch = DecodeUint64(scratch)

		if oldSize == len(scratch) {
			t.Error("Fails to decode")
		}

		if res != val {
			t.Error("decode to a wrong value ", res)
		}
	}
}

func TestEncodeAndDecodeSlice(t *testing.T) {
	scratch := make([]byte, 0)
	data := [...][]byte{
		[]byte("hello, world"),
		[]byte("this is go programming"),
		[]byte("sdb"),
	}

	for _, val := range data {
		scratch = EncodeSlice(scratch, val)
	}

	for _, val := range data {
		var res []byte
		oldSize := len(scratch)

		res, scratch = DecodeSlice(scratch)

		if oldSize == len(scratch) {
			t.Error("Fails to decode")
		}

		if bytes.Compare(res, val) != 0 {
			t.Error("decode to a wrong value ", string(res))
		}
	}
}

func TestEncodeAndDecodeVarInt(t *testing.T) {
	scratch := make([]byte, 0)
	data := [...]uint64{876501232345, 34, 123456, 345}

	for _, val := range data {
		scratch = EncodeVarInt(scratch, val)
	}

	for _, val := range data {
		var res uint64
		oldSize := len(scratch)
		res, scratch = DecodeVarInt(scratch)

		if oldSize == len(scratch) {
			t.Error("Fails to decode")
		}

		if res != val {
			t.Error("decode a wrong value ", res, " expected ", val)
		}
	}
}

func TestEncodeDecodeMultiObjects(t *testing.T) {
	scratch := make([]byte, 0)

	scratch = EncodeUint32(scratch, uint32(45))
	scratch = EncodeUint64(scratch, uint64(123400004321))
	scratch = EncodeSlice(scratch, []byte("hello, world"))
	scratch = EncodeUint32(scratch, uint32(12))

	{
		var val uint32
		val, scratch = DecodeUint32(scratch)

		if val != uint32(45) {
			t.Error("fails to decode value")
		}
	}

	{
		var val uint64
		val, scratch = DecodeUint64(scratch)

		if val != uint64(123400004321) {
			t.Error("fails to decode value")
		}
	}

	{
		var val []byte
		val, scratch = DecodeSlice(scratch)

		if bytes.Compare(val, []byte("hello, world")) != 0 {
			t.Error("fails to decode value")
		}
	}

	{
		var val uint32
		val, scratch = DecodeUint32(scratch)

		if val != uint32(12) {
			t.Error("fails to decode value")
		}
	}
}
