package db

import (
	"unsafe"
)

// Like levelDB, a table is a storage unit that persistently keeps
// certain amount of sorted key value pairs. A table usually
// corresponds to a nsst file ("not a sst file", similar to levelDB's
// sst file, but in a different format) in file system.
//
// Table uses differential encoding for keys. A table has two types
// of block, a single index block and multiple leaf blocks.
// The key of index blocks are full keys, while the keys of leaf
// blocks are partial keys (differential encoded in regard of previous
// keys) The value field of an entry in index block is an offset to
// corresponding entries in leaf block

const (
	// how frequent a full key should appear in leaf block
	kEntriesPerFullKey = 8
	// Suppose each table has 2MB data and each entry is 100 bytes,
	// a table will have 20K entries and we will have 40 entries
	// in index block
	kNumLeafEntriesPerIndexEntry = 512
)

// Differentiate encoding: given previous and current key,
// generate differentiate bytes for current key
func EncodeDifferentialKey(prev, current []byte) []byte {
	var short int
	if len(prev) > len(current) {
		short = len(current)
	} else {
		short = len(prev)
	}

	common := short
	for i := 0; i < short; i++ {
		if prev[i] != current[i] {
			common = i
			break
		}
	}

	// only use a single byte to store the common length
	if common > 127 {
		common = 127
	}

	ret := make([]byte, len(current)-common+1)
	*(*uint8)(unsafe.Pointer(&ret[0])) = uint8(common)
	copy(ret[1:], current[common:])

	return ret
}

// Differential decoding: given previous full code and a differential
// coded key, restore corresponding full key
func DecodeDifferentialKey(prev, current []byte) []byte {
	common := *(*uint8)(unsafe.Pointer(&current[0]))
	ret := make([]byte, int(common)+len(current)-1)
	if common > 0 {
		copy(ret, prev[:common])
	}

	copy(ret[common:], current[1:])
	return ret
}

// leaf blocks use differential encoded key. This iterator is used
// to decode partial keys in leaf block so that the returned
// key value from Key() is always a full key
type DifferentialDecodingIter struct {
	blockIter Iterator
	prevKey   []byte
}

func (it *DifferentialDecodingIter) Valid() bool {
	return it.blockIter.Valid()
}

func (it *DifferentialDecodingIter) Value() []byte {
	return it.blockIter.Value()
}

func (it *DifferentialDecodingIter) SeekToFirst() {
	it.blockIter.SeekToFirst()
	it.prevKey = nil
}

func (it *DifferentialDecodingIter) SeekToLast() {
	it.blockIter.SeekToLast()
	it.prevKey = nil
}

func (it *DifferentialDecodingIter) Seek(key []byte) {
	it.blockIter.Seek(key)
	it.prevKey = nil
}

func (it *DifferentialDecodingIter) Next() {
	it.prevKey = it.Key()
	it.blockIter.Next()
}

func (it *DifferentialDecodingIter) Prev() {
	it.blockIter.Prev()
	it.prevKey = nil
}

func (it *DifferentialDecodingIter) Key() []byte {
	for true {
		if it.prevKey != nil {
			return DecodeDifferentialKey(it.prevKey, it.blockIter.Key())
		} else {
			// previous key is not available, search backward for it
			var curKey []byte
			backoff := 0
			for true {
				curKey = it.blockIter.Key()
				if *(*uint8)(unsafe.Pointer(&curKey[0])) == uint8(0) {
					break
				} else {
					backoff = backoff + 1
					it.Prev()
					if !it.Valid() {
						panic("reach the beginning before a full key")
					}
				}
			}

			// derive latter keys from the nearest full key
			for ; backoff > 1; backoff-- {
				it.Next()
				curKey = DecodeDifferentialKey(curKey, it.blockIter.Key())
			}

			it.prevKey = curKey

			// restore raw iterator to original position
			if backoff > 0 {
				it.Next()
			}
		}
	}

	panic("should not reach here")
	return nil
}

// Struct used to build a table. A table is an immutable construct.
// Once it is build, no changes should be applied to it later.
type TableBuilder struct {
	sizeHint     int
	leafData     []byte
	indexData    []byte
	leafNumber   uint32
	indexSize    uint32
	numEntries   uint32
	leafPos      uint32
	firstKey     []byte
	prevKey      []byte
	file         WritableFile
	leafBuilder  *BlockBuilder
	indexBuilder *BlockBuilder
}

// Create a new table builder.
func MakeTableBuilder(f WritableFile, sizeHint int) *TableBuilder {
	data1 := make([]byte, sizeHint)
	data2 := make([]byte, sizeHint)

	return &TableBuilder{
		sizeHint:     sizeHint,
		leafData:     data1,
		indexData:    data2,
		file:         f,
		leafBuilder:  MakeBlockBuilder(data1),
		indexBuilder: MakeBlockBuilder(data2),
	}
}

// Add a new entry to the table to be built.
func (a *TableBuilder) Add(key, value []byte) {
	// Each table has an upper limit on storage capacity that is determined
	// by @sizeHint. Caller should make sure that a table never exceed
	// this limit.
	estimate := len(key) + len(value)
	if estimate+int(a.leafPos)+64 > a.sizeHint {
		panic("Too many payload in a table!")
	}

	a.numEntries++
	for true {
		if a.leafNumber < kNumLeafEntriesPerIndexEntry {
			if a.firstKey == nil {
				a.firstKey = key
			}
			residual := a.leafNumber % kEntriesPerFullKey
			var newKey []byte
			if residual != 0 {
				newKey = EncodeDifferentialKey(a.prevKey, key)
			} else {
				newKey = make([]byte, len(key)+1)
				copy(newKey[1:], key)
			}
			a.leafBuilder.Add(newKey, value)
			a.prevKey = key
			a.leafNumber = a.leafNumber + 1
			break
		} else {
			b, ok := a.leafBuilder.Finalize()
			if !ok {
				panic("leaf builder fails to finalize")
			}
			off := uint32(len(b.data))
			a.leafPos = a.leafPos + off
			indexValue := make([]byte, 4)
			*(*uint32)(unsafe.Pointer(&indexValue[0])) = off
			a.indexBuilder.Add(key, indexValue)
			a.leafNumber = 0
		}
	}
}

func (a *TableBuilder) Finalize(c Comparator) *Table {
	b, ok := a.leafBuilder.Finalize()
	if !ok {
		panic("leaf builder fails to finalize")
	}
	off := uint32(len(b.data))
	a.leafPos = a.leafPos + off
	indexValue := make([]byte, 4)
	*(*uint32)(unsafe.Pointer(&indexValue[0])) = off
	a.indexBuilder.Add(a.prevKey, indexValue)

	b, ok = a.indexBuilder.Finalize()
	if !ok {
		panic("index builder fails to finalize")
	}
	a.indexSize = uint32(len(b.data))

	// format of a table file: first part is many leaf blocks
	status := a.file.Append(a.leafData[:a.leafPos])
	if !status.Ok() {
		panic("fails to write to table file")
	}

	// second part of table file: a final index block
	status = a.file.Append(a.indexData[:a.indexSize])
	if !status.Ok() {
		panic("fails to write to table file")
	}

	ret := &Table{b, a.leafData, c}
	return ret
}

// Abort building the table, close opened file descriptors.
func (a *TableBuilder) Abort() {
	a.file.Close()
}

type Table struct {
	index      *Block
	leafData   []byte
	comparator Comparator
}

// read table from disk file. Pass in a buffer that is the same
// size as the file size
func RecoverTable(file SequentialFile, buffer []byte, c Comparator) *Table {
	used, status := file.Read(buffer)
	if !status.Ok() || len(used) != len(buffer) {
		return nil
	}

	pos := len(used)
	ret := &Table{}

	ret.comparator = c
	ret.leafData = used
	ret.index = DecodeBlock(used, uint32(pos))

	return ret
}

func (t *Table) NewIterator() Iterator {
	ret := &TableIter{}
	ret.table = t
	ret.indexIter = t.index.NewIterator(t.comparator)
	return ret
}

// This iterator composite an index block iterator and leaf block
// iterators
type TableIter struct {
	table     *Table
	leafBlock *Block
	indexIter Iterator
	leafIter  *DifferentialDecodingIter
	valid     bool
}

func (it *TableIter) Valid() bool {
	return it.valid
}

func (it *TableIter) SeekToFirst() {
	it.valid = false
	it.indexIter.SeekToFirst()
	if it.indexIter.Valid() {
		val := it.indexIter.Value()
		lastOff := *(*uint32)(unsafe.Pointer(&val[0]))
		it.leafBlock = DecodeBlock(it.table.leafData, lastOff)
		if it.leafBlock != nil {
			rawIt := it.leafBlock.NewIterator(it.table.comparator)
			it.leafIter = &DifferentialDecodingIter{rawIt, nil}
			it.leafIter.SeekToFirst()
			if it.leafIter.Valid() {
				it.valid = true
			}
		}
	}
}

func (it *TableIter) SeekToLast() {
	it.valid = false
	it.indexIter.SeekToLast()
	if it.indexIter.Valid() {
		val := it.indexIter.Value()
		lastOff := *(*uint32)(unsafe.Pointer(&val[0]))
		it.leafBlock = DecodeBlock(it.table.leafData, lastOff)
		if it.leafBlock != nil {
			rawIter := it.leafBlock.NewIterator(it.table.comparator)
			it.leafIter = &DifferentialDecodingIter{rawIter, nil}
			it.leafIter.SeekToLast()
			if it.leafIter.Valid() {
				it.valid = true
			}
		}
	}
}

func (it *TableIter) Seek(key []byte) {
	it.valid = false
	it.indexIter.Seek(key)
	if it.indexIter.Valid() {
		val := it.indexIter.Value()
		lastOff := *(*uint32)(unsafe.Pointer(&val[0]))
		it.leafBlock = DecodeBlock(it.table.leafData, lastOff)
		if it.leafBlock != nil {
			rawIt := it.leafBlock.NewIterator(it.table.comparator)
			it.leafIter = &DifferentialDecodingIter{rawIt, nil}
			it.leafIter.Seek(key)
			if it.leafIter.Valid() {
				it.valid = true
			}
		}
	}
}

func (it *TableIter) Next() {
	// Next() and Prev() should be called only if the iterator is valid
	// If one wants to scan to the end and change direction, he should
	// use SeekToFirst() or SeekToLast() before Next() or Prev() is
	// called
	if !it.valid {
		panic("iterator is not valid")
	}

	it.leafIter.Next()
	if !it.leafIter.Valid() {
		it.valid = false
		it.indexIter.Next()
		if it.indexIter.Valid() {
			val := it.indexIter.Value()
			lastOff := *(*uint32)(unsafe.Pointer(&val[0]))
			it.leafBlock = DecodeBlock(it.table.leafData, lastOff)

			if it.leafBlock != nil {
				rawIter := it.leafBlock.NewIterator(it.table.comparator)
				it.leafIter = &DifferentialDecodingIter{rawIter, nil}
				it.leafIter.SeekToFirst()
				if it.leafIter.Valid() {
					it.valid = true
				}
			}
		}
	}
}

func (it *TableIter) Prev() {
	if !it.valid {
		panic("iterator is not valid")
	}

	it.leafIter.Prev()
	if !it.leafIter.Valid() {
		it.valid = false
		it.indexIter.Prev()
		if it.indexIter.Valid() {
			val := it.indexIter.Value()
			lastOff := *(*uint32)(unsafe.Pointer(&val[0]))
			it.leafBlock = DecodeBlock(it.table.leafData, lastOff)

			if it.leafBlock != nil {
				rawIter := it.leafBlock.NewIterator(it.table.comparator)
				it.leafIter = &DifferentialDecodingIter{rawIter, nil}
				it.leafIter.SeekToLast()
				if it.leafIter.Valid() {
					it.valid = true
				}
			}
		}
	}
}

func (it *TableIter) Key() []byte {
	return it.leafIter.Key()
}

func (it *TableIter) Value() []byte {
	return it.leafIter.Value()
}
