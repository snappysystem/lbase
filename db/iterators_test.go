package db

import (
	"sort"
	"testing"
)

// An iterator class that walk through a slice of strings.
type SliceIterator struct {
	name string // Use as value field.
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
	return []byte(it.name)
}

func (it *SliceIterator) Close() {
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

func TestHeapIteratorZigZag(t *testing.T) {
	l0 := &SliceIterator{vals: []string{"1004"}, idx: 0}
	l1 := &SliceIterator{vals: []string{"1000"}, idx: 0}
	l2 := &SliceIterator{vals: []string{"1001"}, idx: 0}
	l3 := &SliceIterator{vals: []string{"1002"}, idx: 0}
	l4 := &SliceIterator{vals: []string{"1003"}, idx: 0}

	iters := []Iterator{}

	iters = append(iters, l0)
	iters = append(iters, l1)
	iters = append(iters, l2)
	iters = append(iters, l3)
	iters = append(iters, l4)

	it := MakeHeapIterator(iters, ByteOrder(0))
	it.SeekToFirst()

	expected := []string{
		"1000",
		"1001",
		"1002",
		"1003",
		"1004",
	}

	for _, key := range expected {
		if !it.Valid() {
			t.Error("iterator is not valid")
		}
		if string(it.Key()) != key {
			t.Error("expect ", key, " got ", string(it.Key()))
		}

		it.Prev()
		if it.Valid() {
			it.Next()
		} else {
			it.SeekToFirst()
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

func TestHeapIteratorHideLevels(t *testing.T) {
	l0 := &SliceIterator{name: "first", vals: []string{"hello", "test"}, idx: 0}
	l1 := &SliceIterator{name: "second", vals: []string{"abc", "test"}, idx: 0}
	l2 := &SliceIterator{name: "third", vals: []string{"zero"}, idx: 0}

	iters := []Iterator{}

	iters = append(iters, l0)
	iters = append(iters, l1)
	iters = append(iters, l2)

	it := MakeHeapIterator(iters, ByteOrder(0))
	it.Seek([]byte("test"))

	if !it.Valid() {
		t.Error("did not find the key")
	}

	if string(it.Key()) != "test" || string(it.Value()) != "first" {
		t.Error("expectation does not meet")
	}

	it.Next()

	if !it.Valid() {
		t.Error("did not find the key")
	}

	if string(it.Key()) != "zero" {
		t.Error("expectation does not meet")
	}

	it.Prev()

	if !it.Valid() {
		t.Error("did not find the key")
	}

	if string(it.Value()) != "first" {
		t.Error("expectation does not meet", string(it.Value()))
	}
}

func TestConcatenationIteratorMove(t *testing.T) {
	l0 := &SliceIterator{vals: []string{"abc", "hello"}}
	l1 := &SliceIterator{vals: []string{"play", "quick"}}
	l2 := &SliceIterator{vals: []string{"zero"}}

	iter := &ConcatenationIterator{iters: []Iterator{l0, l1, l2}}
	iter.SeekToFirst()

	expection := []string{"abc", "hello", "play", "quick", "zero"}
	for _, val := range expection {
		if !iter.Valid() {
			t.Error("Expect more entries!")
		}
		if string(iter.Key()) != val {
			t.Error("Wrong key found!", string(iter.Key()), " expect ", val)
		}
		iter.Next()
	}

	if iter.Valid() {
		t.Error("Expect to be end at this point!")
	}

	iter.SeekToLast()
	for i := len(expection) - 1; i >= 0; i-- {
		val := expection[i]
		if !iter.Valid() {
			t.Error("Expect more entries!")
		}
		if string(iter.Key()) != val {
			t.Error("Wrong key found!")
		}
		iter.Prev()
	}
}

func TestConcatenationIteratorZigZag(t *testing.T) {
	l0 := &SliceIterator{vals: []string{"abc", "hello"}}
	l1 := &SliceIterator{vals: []string{"play", "quick"}}

	iter := &ConcatenationIterator{iters: []Iterator{l0, l1}}
	iter.SeekToFirst()

	expection := []string{"abc", "hello", "play", "quick"}
	for _, val := range expection {
		if !iter.Valid() {
			t.Error("Expect more entries!")
		}
		if string(iter.Key()) != val {
			t.Error("Wrong key found!", string(iter.Key()), " expect ", val)
		}

		iter.Prev()
		if iter.Valid() {
			iter.Next()
		} else {
			iter.SeekToFirst()
		}

		iter.Next()
	}

	if iter.Valid() {
		t.Error("Expect to be end at this point!")
	}
}

func TestConcatenationIteratorSeek(t *testing.T) {
	l0 := &SliceIterator{vals: []string{"abc", "hello"}}
	l1 := &SliceIterator{vals: []string{"play", "quick"}}
	l2 := &SliceIterator{vals: []string{"zero"}}

	iter := &ConcatenationIterator{iters: []Iterator{l0, l1, l2}}

	iter.Seek([]byte("play"))
	if !iter.Valid() || string(iter.Key()) != "play" {
		t.Error("Did not find the key!")
	}

	iter.Seek([]byte("somthing"))
	if !iter.Valid() || string(iter.Key()) != "zero" {
		t.Error("Did not find the key!")
	}
}
