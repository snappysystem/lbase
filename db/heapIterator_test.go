package db

import (
	"sort"
	"testing"
)

// An iterator class that walk through a slice of strings.
type SliceIterator struct {
	vals []string
	idx  int
}

func (it *SliceIterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.vals)
}

func (it *SliceIterator) SeekToFirst() {
	it.idx = 0
}

func (it *SliceIterator) SeekToLast() {
	it.idx = len(it.vals) - 1
}

func (it *SliceIterator) Seek(key []byte) {
	it.idx = sort.SearchStrings(it.vals, string(key))
}

func (it *SliceIterator) Next() {
	it.idx++
}

func (it *SliceIterator) Prev() {
	it.idx--
}

func (it *SliceIterator) Key() []byte {
	return []byte(it.vals[it.idx])
}

func (it *SliceIterator) Value() []byte {
	return it.Key()
}

func TestHeapIteratorForward(t *testing.T) {
	l0 := &SliceIterator{vals: []string{"hello", "test"}, idx: 0}
	l1 := &SliceIterator{vals: []string{"abc", "sample"}, idx: 0}
	l2 := &SliceIterator{vals: []string{"play"}, idx: 0}

	iters := []Iterator{}

	iters = append(iters, l0)
	iters = append(iters, l1)
	iters = append(iters, l2)

	it := MakeHeapIterator(iters, ByteOrder(0))

	expected := []string{
		"abc",
		"hello",
		"play",
		"sample",
		"test",
	}

	for _, key := range expected {
		if !it.Valid() {
			t.Error("iterator is not valid")
		}
		if string(it.Key()) != key {
			t.Error("expect ", key, " got ", string(it.Key()))
		}

		it.Next()
	}
}

func TestHeapIteratorBackward(t *testing.T) {
	l0 := &SliceIterator{vals: []string{"hello", "test"}, idx: 0}
	l1 := &SliceIterator{vals: []string{"abc", "sample"}, idx: 0}
	l2 := &SliceIterator{vals: []string{"play"}, idx: 0}

	iters := []Iterator{}

	iters = append(iters, l0)
	iters = append(iters, l1)
	iters = append(iters, l2)

	it := MakeHeapIterator(iters, ByteOrder(0))
	it.SeekToLast()

	expected := []string{
		"test",
		"sample",
		"play",
		"hello",
		"abc",
	}

	for _, key := range expected {
		if !it.Valid() {
			t.Error("iterator is not valid")
		}
		if string(it.Key()) != key {
			t.Error("expect ", key, " got ", string(it.Key()))
		}

		it.Prev()
	}
}

func TestHeapIteratorMoveAround(t *testing.T) {
	l0 := &SliceIterator{vals: []string{"hello", "test"}, idx: 0}
	l1 := &SliceIterator{vals: []string{"abc", "sample"}, idx: 0}
	l2 := &SliceIterator{vals: []string{"play"}, idx: 0}

	iters := []Iterator{}

	iters = append(iters, l0)
	iters = append(iters, l1)
	iters = append(iters, l2)

	it := MakeHeapIterator(iters, ByteOrder(0))
	it.Seek([]byte("play"))

	if !it.Valid() {
		t.Error("Iterator is not valid")
	}

	if string(it.Key()) != "play" {
		t.Error("Key does not match")
	}

	it.Next()

	if !it.Valid() {
		t.Error("Iterator is not valid")
	}

	if string(it.Key()) != "sample" {
		t.Error("Key does not match")
	}

	it.Prev()

	if !it.Valid() {
		t.Error("Iterator is not valid")
	}

	if string(it.Key()) != "play" {
		t.Error("Key does not match ", string(it.Key()))
	}

	it.Next()

	if !it.Valid() {
		t.Error("Iterator is not valid")
	}

	if string(it.Key()) != "sample" {
		t.Error("Key does not match")
	}
}
