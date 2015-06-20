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

// NOTE: cgo does not support LDFLAGS:-static very well.
// So the best way to build dependent library (i.e. leveldb)
// is to use "make libleveldb.a" instead of "make".
// The former only build a static library and force cgo
// to use that static library.

/*
#cgo CFLAGS: -I../externals/leveldb
#cgo LDFLAGS: -L../externals/leveldb -lleveldb -lstdc++

#include "include/leveldb/c.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"unsafe"
)

// Define leveldb options.
type DbOptions struct {
	impl *C.leveldb_options_t
}

func NewDbOptions() *DbOptions {
	return &DbOptions{
		impl: C.leveldb_options_create(),
	}
}

func (opts *DbOptions) Destroy() {
	C.leveldb_options_destroy(opts.impl)
}

func (opts *DbOptions) SetCreateIfMissing(val int) {
	C.leveldb_options_set_create_if_missing(opts.impl, C.uchar(val))
}

func (opts *DbOptions) SetErrorIfExists(val int) {
	C.leveldb_options_set_error_if_exists(opts.impl, C.uchar(val))
}

func (opts *DbOptions) SetParanoidChecks(val int) {
	C.leveldb_options_set_paranoid_checks(opts.impl, C.uchar(val))
}

func (opts *DbOptions) SetWriteBufferSize(s int) {
	C.leveldb_options_set_write_buffer_size(opts.impl, C.size_t(s))
}

func (opts *DbOptions) SetMaxOpenFiles(numFiles int) {
	C.leveldb_options_set_max_open_files(opts.impl, C.int(numFiles))
}

func (opts *DbOptions) SetBlockSize(s int) {
	C.leveldb_options_set_block_size(opts.impl, C.size_t(s))
}

func (opts *DbOptions) SetBlockRestartInterval(val int) {
	C.leveldb_options_set_block_restart_interval(opts.impl, C.int(val))
}

// Define leveldb write options.
type WriteOptions struct {
	impl *C.leveldb_writeoptions_t
}

func NewWriteOptions() WriteOptions {
	return WriteOptions{
		impl: C.leveldb_writeoptions_create(),
	}
}

func (opts WriteOptions) Destroy() {
	C.leveldb_writeoptions_destroy(opts.impl)
}

func (opts WriteOptions) SetSync(sync int) {
	C.leveldb_writeoptions_set_sync(opts.impl, C.uchar(sync))
}

// Define snapshot type.
type Snapshot struct {
	impl *C.leveldb_snapshot_t
}

// Define leveldb read options.
type ReadOptions struct {
	impl *C.leveldb_readoptions_t
}

func NewReadOptions() ReadOptions {
	return ReadOptions{
		impl: C.leveldb_readoptions_create(),
	}
}

func (opts ReadOptions) Destroy() {
	C.leveldb_readoptions_destroy(opts.impl)
}

func (opts ReadOptions) SetVerifyChecksums(set int) {
	C.leveldb_readoptions_set_verify_checksums(opts.impl, C.uchar(set))
}

func (opts ReadOptions) SetFillCache(set int) {
	C.leveldb_readoptions_set_fill_cache(opts.impl, C.uchar(set))
}

func (opts ReadOptions) SetSnapshot(s Snapshot) {
	C.leveldb_readoptions_set_snapshot(opts.impl, s.impl)
}

// Define WriteBatch class.
type WriteBatch struct {
	impl *C.leveldb_writebatch_t
}

func NewWriteBatch() WriteBatch {
	return WriteBatch{
		impl: C.leveldb_writebatch_create(),
	}
}

func (w WriteBatch) Destroy() {
	C.leveldb_writebatch_destroy(w.impl)
}

func (w WriteBatch) Clear() {
	C.leveldb_writebatch_clear(w.impl)
}

func (w WriteBatch) Put(key, val []byte) {
	keyPtr := (*C.char)(unsafe.Pointer(&key[0]))
	valPtr := (*C.char)(unsafe.Pointer(&val[0]))
	lk := C.size_t(len(key))
	lv := C.size_t(len(val))
	C.leveldb_writebatch_put(w.impl, keyPtr, lk, valPtr, lv)
}

func (w WriteBatch) Delete(key []byte) {
	keyPtr := (*C.char)(unsafe.Pointer(&key[0]))
	keyLen := C.size_t(len(key))
	C.leveldb_writebatch_delete(w.impl, keyPtr, keyLen)
}

// Leveldb iterator.
type Iterator struct {
	impl *C.leveldb_iterator_t
}

func (it Iterator) Destroy() {
	C.leveldb_iter_destroy(it.impl)
}

func (it Iterator) Valid() bool {
	if C.leveldb_iter_valid(it.impl) != 0 {
		return true
	} else {
		return false
	}
}

func (it Iterator) SeekToFirst() {
	C.leveldb_iter_seek_to_first(it.impl)
}

func (it Iterator) SeekToLast() {
	C.leveldb_iter_seek_to_last(it.impl)
}

func (it Iterator) Seek(key []byte) {
	kPtr := (*C.char)(unsafe.Pointer(&key[0]))
	C.leveldb_iter_seek(it.impl, kPtr, C.size_t(len(key)))
}

func (it Iterator) Next() {
	C.leveldb_iter_next(it.impl)
}

func (it Iterator) Prev() {
	C.leveldb_iter_prev(it.impl)
}

func (it Iterator) Key() []byte {
	var klen C.size_t
	ptr := C.leveldb_iter_key(it.impl, &klen)
	if ptr != nil {
		return C.GoBytes(unsafe.Pointer(ptr), C.int(klen))
	} else {
		return nil
	}
}

func (it Iterator) Value() []byte {
	var vlen C.size_t
	ptr := C.leveldb_iter_value(it.impl, &vlen)
	if ptr != nil {
		return C.GoBytes(unsafe.Pointer(ptr), C.int(vlen))
	} else {
		return nil
	}
}

func (it Iterator) GetError() error {
	var errptr *C.char
	C.leveldb_iter_get_error(it.impl, &errptr)
	if errptr == nil {
		return nil
	} else {
		msg := C.GoString(errptr)
		C.free(unsafe.Pointer(errptr))
		return errors.New(msg)
	}
}

// Define Db class.
type Db struct {
	impl *C.leveldb_t
}

func OpenDb(options *DbOptions, name string) (db Db, err error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var errptr *C.char
	db.impl = C.leveldb_open(options.impl, cname, &errptr)
	if errptr != nil {
		msg := C.GoString(errptr)
		C.free(unsafe.Pointer(errptr))
		err = errors.New(msg)
	}
	return
}

func (db Db) Close() {
	C.leveldb_close(db.impl)
}

func (db Db) Put(opts WriteOptions, key, val []byte) error {
	keyPtr := (*C.char)(unsafe.Pointer(&key[0]))
	valPtr := (*C.char)(unsafe.Pointer(&val[0]))
	keyLen := C.size_t(len(key))
	valLen := C.size_t(len(val))
	var errptr *C.char
	C.leveldb_put(db.impl, opts.impl, keyPtr, keyLen, valPtr, valLen, &errptr)
	if errptr == nil {
		return nil
	} else {
		msg := C.GoString(errptr)
		C.free(unsafe.Pointer(errptr))
		return errors.New(msg)
	}
}

func (db Db) Delete(opts WriteOptions, key []byte) error {
	keyPtr := (*C.char)(unsafe.Pointer(&key[0]))
	keyLen := C.size_t(len(key))
	var errptr *C.char
	C.leveldb_delete(db.impl, opts.impl, keyPtr, keyLen, &errptr)
	if errptr == nil {
		return nil
	} else {
		msg := C.GoString(errptr)
		C.free(unsafe.Pointer(errptr))
		return errors.New(msg)
	}
}

func (db Db) Write(opts WriteOptions, batch WriteBatch) error {
	var errptr *C.char
	C.leveldb_write(db.impl, opts.impl, batch.impl, &errptr)
	if errptr == nil {
		return nil
	} else {
		msg := C.GoString(errptr)
		C.free(unsafe.Pointer(errptr))
		return errors.New(msg)
	}
}

func (db Db) Get(opts ReadOptions, key []byte) (val []byte, e error) {
	keyPtr := (*C.char)(unsafe.Pointer(&key[0]))
	keyLen := C.size_t(len(key))
	var vallen C.size_t
	var errptr *C.char
	cPtr := C.leveldb_get(db.impl, opts.impl, keyPtr, keyLen, &vallen, &errptr)
	if cPtr != nil {
		val = C.GoBytes(unsafe.Pointer(cPtr), C.int(vallen))
	}
	if errptr != nil {
		msg := C.GoString(errptr)
		C.free(unsafe.Pointer(errptr))
		e = errors.New(msg)
	}
	return
}

func (db Db) CreateIterator(opts ReadOptions) Iterator {
	return Iterator{
		impl: C.leveldb_create_iterator(db.impl, opts.impl),
	}
}

func (db Db) CreateSnapshot() Snapshot {
	return Snapshot{
		impl: C.leveldb_create_snapshot(db.impl),
	}
}

func (db Db) ReleaseSnapshot(s Snapshot) {
	C.leveldb_release_snapshot(db.impl, s.impl)
}

// Returns NULL if property name is unknown.
// Else returns a pointer to a malloc()-ed null-terminated value.
func (db Db) PropertyValue(propname string) (ret string, found bool) {
	cname := C.CString(propname)
	defer C.free(unsafe.Pointer(cname))
	valPtr := C.leveldb_property_value(db.impl, cname)
	if valPtr != nil {
		ret, found = C.GoString(valPtr), true
	}
	return
}

func (db Db) ApproximateSizes(startKeys, limitKeys []string) []int64 {
	numRange := len(startKeys)
	cStartKeys := make([]*C.char, numRange)
	cLimitKeys := make([]*C.char, numRange)
	startLens := make([]C.size_t, numRange)
	limitLens := make([]C.size_t, numRange)
	ret := make([]int64, numRange)

	for i := 0; i < numRange; i++ {
		cStartKeys[i] = C.CString(startKeys[i])
		cLimitKeys[i] = C.CString(limitKeys[i])
		startLens[i] = C.size_t(len(startKeys[i]))
		limitLens[i] = C.size_t(len(limitKeys[i]))
	}

	defer func() {
		for i := 0; i < numRange; i++ {
			C.free(unsafe.Pointer(cStartKeys[i]))
			C.free(unsafe.Pointer(cLimitKeys[i]))
		}
	}()

	C.leveldb_approximate_sizes(
		db.impl,
		C.int(numRange),
		(**C.char)(unsafe.Pointer(&cStartKeys[0])),
		(*C.size_t)(unsafe.Pointer(&startLens[0])),
		(**C.char)(unsafe.Pointer(&cLimitKeys[0])),
		(*C.size_t)(unsafe.Pointer(&limitLens[0])),
		(*C.uint64_t)(unsafe.Pointer(&ret[0])))

	return ret
}

func (db Db) CompactRange(startKey, limitKey []byte) {
	startPtr := (*C.char)(unsafe.Pointer(&startKey[0]))
	limitPtr := (*C.char)(unsafe.Pointer(&limitKey[0]))
	startLen := C.size_t(len(startKey))
	limitLen := C.size_t(len(limitKey))
	C.leveldb_compact_range(db.impl, startPtr, startLen, limitPtr, limitLen)
}

func DestroyDb(opts DbOptions, name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var errptr *C.char
	C.leveldb_destroy_db(opts.impl, cname, &errptr)
	if errptr != nil {
		msg := C.GoString(errptr)
		C.free(unsafe.Pointer(errptr))
		return errors.New(msg)
	} else {
		return nil
	}
}

func RepairDb(opts DbOptions, name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var errptr *C.char
	C.leveldb_repair_db(opts.impl, cname, &errptr)
	if errptr != nil {
		msg := C.GoString(errptr)
		C.free(unsafe.Pointer(errptr))
		return errors.New(msg)
	} else {
		return nil
	}
}
