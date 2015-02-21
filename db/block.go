package db

// Implement a "block" in db table. Like levelDB, a table consists
// of multiple blocks, each of which can be used as a storage
// for a set of sorted key value pair.

// For efficiency reason, this implementation does not convert
// integers into big endian format in a file. This is usually
// fine since we are only use the same data locally which is
// very unlikely to have an endianess change. But if uses arise
// that requires physically copy the data to other servers
// which potentially have different kind of CPUs, we may have
// to revisit the decision and implement a endianess aware
// implementation

import (
	"sort"
	"unsafe"
)

// A struct to build a "block". A block is an immutable construct.
// Once it is build, there is no more changes that can be applied
// to it.
type BlockBuilder struct {
	data []byte
	cur  uint32
	keys []uint32
}

// A tailer of block, always at the end of a block
type blockTailer struct {
	blockSize     uint32
	numKeys       uint32
	restartOffset uint32
}

// In-memory representation of a block
//
// A block is a consecutive area in disk space, possibly
// within a file. The layout of a block on disk space:
//
//  (1) A list of key value pairs
//  (2) A list of fixed sized offsets, which point to
//      the start offset of each key value pair in the block.
//      The starting offset of this list is @restartOffset.
//  (3) A block tailer described above
type Block struct {
	data          []byte
	restartOffset uint32
	numKeys       uint32
}

// An iterator to traverse data entries in a block
type blockIter struct {
	block *Block
	order Comparator
	idx   int32
}

// Parse a key value pair from particular location of a block.
// The layout of underlying data is like:
// [key length][value length][key][value]
// Also return how many bytes have been consumed during parsing.
func parseSimpleEntry(data []byte, off uint32) (key, val []byte, s uint32) {
	keylen := uint32(0)
	vallen := uint32(0)
	pos := int(off)

	// parse key length
	{
		left := data[pos:]
		v, r := DecodeVarInt(left)

		// abort if we fails to decode
		l := len(left) - len(r)
		if l <= 0 {
			return
		}

		keylen = uint32(v)
		s = s + uint32(l)
		pos = pos + l
	}

	// parse value length
	{
		left := data[pos:]
		v, r := DecodeVarInt(left)

		// abort if we fails to decode
		l := len(left) - len(r)
		if l <= 0 {
			return
		}

		vallen = uint32(v)
		s = s + uint32(l)
		pos = pos + l
	}

	// parse key
	{
		key = data[pos : pos+int(keylen)]
		s = s + keylen
		pos = pos + int(keylen)
	}

	// parse value
	{
		val = data[pos : pos+int(vallen)]
		s = s + vallen
		pos = pos + int(vallen)
	}

	return
}

// create a new iterator for the block
func (a *Block) NewIterator(o Comparator) Iterator {
	return &blockIter{
		block: a,
		order: o,
		idx:   -1,
	}
}

func (a *blockIter) Valid() bool {
	return (a.idx >= 0 && a.idx < int32(a.block.numKeys))
}

func (a *blockIter) SeekToFirst() {
	if a.block.numKeys >= 0 {
		a.idx = 0
	} else {
		a.idx = -1
	}
}

func (a *blockIter) SeekToLast() {
	a.idx = int32(a.block.numKeys)
}

// Find and point to the key. If key does not exist, point to the
// key that immediately follow @key in the index
func (a *blockIter) Seek(mark []byte) {
	b := a.block
	a.idx = int32(sort.Search(
		int(b.numKeys),
		func(n int) bool {
			loc := &b.data[int(b.restartOffset)+n*4]
			offsetPtr := (*uint32)(unsafe.Pointer(loc))
			key, _, consumed := parseSimpleEntry(b.data, *offsetPtr)
			if consumed == 0 {
				panic("corrupted data")
			}

			return (a.order.Compare(key, mark) >= 0)
		}))
}

func (a *blockIter) Next() {
	a.idx++
}

func (a *blockIter) Prev() {
	a.idx--
}

func (a *blockIter) Key() []byte {
	b := a.block
	loc := &b.data[b.restartOffset+uint32(a.idx)*4]
	offsetPtr := (*uint32)(unsafe.Pointer(loc))
	key, _, consumed := parseSimpleEntry(b.data, *offsetPtr)
	if consumed == 0 {
		panic("corrupted data")
	}
	return key
}

func (a *blockIter) Value() []byte {
	b := a.block
	loc := &b.data[b.restartOffset+uint32(a.idx)*4]
	offsetPtr := (*uint32)(unsafe.Pointer(loc))
	_, val, consumed := parseSimpleEntry(b.data, *offsetPtr)
	if consumed == 0 {
		panic("corrupted data")
	}
	return val
}

func (a *blockIter) Close() {
}

// create a new BlockBuilder and initialize it
// pass the slice that is going to be used to build the block
func MakeBlockBuilder(data []byte) *BlockBuilder {
	ret := &BlockBuilder{}
	ret.data = data
	ret.keys = make([]uint32, 0, 16*1024)
	return ret
}

// Add a key and a value at a time, return true if success
func (a *BlockBuilder) Add(key []byte, val []byte) bool {
	keylen := len(key)
	vallen := len(val)

	entryOffset := a.cur

	// append key length
	{
		b := a.data[a.cur:a.cur]
		r := EncodeVarInt(b, uint64(keylen))

		if len(r) == 0 {
			return false
		}

		a.cur = a.cur + uint32(len(r))
	}

	// append value length
	{
		b := a.data[a.cur:a.cur]
		r := EncodeVarInt(b, uint64(vallen))

		if len(r) == 0 {
			return false
		}

		a.cur = a.cur + uint32(len(r))
	}

	// append key
	{
		newKey := a.data[a.cur : a.cur+uint32(keylen)]
		s := copy(newKey, key)
		if s != keylen {
			return false
		}
		a.cur = a.cur + uint32(s)
	}

	// append value
	{
		s := copy(a.data[a.cur:a.cur+uint32(vallen)], val)
		if s != vallen {
			return false
		}
		a.cur = a.cur + uint32(s)
	}

	a.keys = append(a.keys, entryOffset)
	return true
}

var modelTailer blockTailer

// Finish building the block, return the slice that denotes
// the boundary of the block. Return true if operation succeeds
func (a *BlockBuilder) Finalize() (ret *Block, ok bool) {
	// align starting of restart offset to 8 byte boundary
	restart := a.cur
	restart = (restart + 7) / 8 * 8
	pos := restart

	// save all key offsets
	for _, off := range a.keys {
		intPtr := (*uint32)(unsafe.Pointer(&a.data[pos]))
		pos = pos + 4
		if int(pos) >= len(a.data) {
			return
		}

		*intPtr = uint32(off)
	}

	// prepare tailer
	tail := (*blockTailer)(unsafe.Pointer(&a.data[pos]))
	pos = pos + uint32(unsafe.Sizeof(modelTailer))
	if int(pos) >= len(a.data) {
		return
	}

	tail.blockSize = pos
	tail.numKeys = uint32(len(a.keys))
	tail.restartOffset = restart

	//prepare result
	ret = &Block{}

	ret.data = a.data[:pos]
	ret.restartOffset = restart
	ret.numKeys = uint32(len(a.keys))

	// reset builder
	a.data = a.data[pos:]
	a.cur = 0
	a.keys = a.keys[:0]

	ok = true

	return
}

// recover a block from a binary slice.
func DecodeBlock(data []byte, endOffset uint32) *Block {
	tailerSize := uint32(unsafe.Sizeof(modelTailer))
	if tailerSize > endOffset {
		return nil
	}

	tail := (*blockTailer)(unsafe.Pointer(&data[endOffset-tailerSize]))
	ret := &Block{}

	// make sure data is valid
	startOffset := endOffset - tail.blockSize
	if startOffset < 0 {
		return nil
	}

	ret.data = data[startOffset:endOffset]
	ret.restartOffset = tail.restartOffset
	ret.numKeys = tail.numKeys

	return ret
}
