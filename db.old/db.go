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

import "bytes"

// A local key value store that closely follow google's level db API.

// A class to enumerate all entries in the key value store
type Iterator interface {
	Valid() bool
	SeekToFirst()
	SeekToLast()
	Seek(key []byte)
	Next()
	Prev()
	Key() []byte
	Value() []byte
	Close()
}

// compare two binaries, return -1 if a is less than b, 0 if a is the same
// as b, and 1 if a is greater than b
type Comparator interface {
	Compare(a []byte, b []byte) int
}

// Types of error messages
const (
	KEY_NOT_FOUND   = "Key not found"
	NOT_IMPLEMENTED = "Not implemented"
)

// A structure that carry's the result of an operation
type Status struct {
	ok, notFound, corruption, ioError bool
	str                               string
}

func MakeStatusOk() Status {
	return Status{
		ok:         true,
		notFound:   false,
		corruption: false,
		ioError:    false,
	}
}

func MakeStatusNotFound(msg string) Status {
	return Status{
		ok:         false,
		notFound:   true,
		corruption: false,
		ioError:    false,
		str:        msg,
	}
}

func MakeStatusCorruption(msg string) Status {
	return Status{
		ok:         false,
		notFound:   false,
		corruption: true,
		ioError:    false,
		str:        msg,
	}
}

func MakeStatusIoError(msg string) Status {
	return Status{
		ok:         false,
		notFound:   false,
		corruption: false,
		ioError:    true,
		str:        msg,
	}
}

func (s *Status) Ok() bool {
	return s.ok
}

func (s *Status) IsNotFound() bool {
	return s.notFound
}

func (s *Status) IsCorruption() bool {
	return s.corruption
}

func (s *Status) IsIoError() bool {
	return s.ioError
}

func (s *Status) ToString() string {
	return s.str
}

// An interface used by db implementation to access OS
// functionality. Caller may supply his own version of
// env when openning a db
type Env interface {
	NewSequentialFile(name string) (SequentialFile, Status)
	NewRandomAccessFile(name string) (RandomAccessFile, Status)
	NewWritableFile(name string) (WritableFile, Status)
	FileExists(name string) bool
	GetChildren(dir string) ([]string, Status)
	DeleteFile(name string) Status
	CreateDir(dir string) Status
	DeleteDir(dir string) Status
	GetFileSize(name string) (uint64, Status)
	RenameFile(src string, target string) Status
}

// define a range [start, limit), note @limit is not included in
// the range
type Range struct {
	start []byte
	limit []byte
}

type Options struct {
}

type ReadOptions struct {
}

type WriteOptions struct {
}

// DB interface
type DB interface {
	Put(opt WriteOptions, key, value []byte) Status
	Delete(opt WriteOptions, key []byte) Status
	Write(opt WriteOptions, updates WriteBatch) Status
	Get(opt ReadOptions, key []byte) ([]byte, Status)
	NewIterator(opt ReadOptions) Iterator
	GetSnapshot() Snapshot
	ReleaseSnapshot(snap Snapshot)
	GetApproximateSizes(ranges []Range) []uint64
	CompactRange(start, limit []byte)
}

type WriteBatch interface {
	Put(key, value []byte)
	Delete(key []byte)
	NewIterator() Iterator
}

type Snapshot interface {
}

type SequentialFile interface {
	Read(scratch []byte) ([]byte, Status)
	Skip(n int64) Status
	Close()
}

type RandomAccessFile interface {
	Read(offset int64, scratch []byte) ([]byte, Status)
	Close()
}

type WritableFile interface {
	Append(data []byte) Status
	Size() int64
	Close() Status
	Flush() Status
	Name() string
}

// Default comparator.
type ByteOrder int

func (x ByteOrder) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}
