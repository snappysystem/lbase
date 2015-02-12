package db

import (
	"container/heap"
)

// HeapIterator composite a slice of iterators into a single one.
// If two iterators in the slice point to the same key, the iteractor
// in lower level will hide the iteractor in higher level.
//
// For example, if a HeapIterator is backed up by a slice of iterators
// that point to sorted int slices:
//
//   [(3,6), (7,7), (10,1) ]
//   [(2,8), (7,9), (8,2) ]
//
// The HeapIterator will walk through the list in following order:
// (2,8), (3,6), (7,7), (8,2), (10,1), note that the pair (7,9)
// is hided by the pair (7,7) in the first level and does not appear
// in the scan run of HeapIterator.

// A structure that captures both iterator and its level in the iterator list.
// HeapIterator uses level information to sort iterators so that if there is
// a tie for keys, lower level wins.
type IteratorPair struct {
	iter  Iterator
	level int
}

// A heap structure for a list of iterators.
type iteratorHeap struct {
	iters   []IteratorPair
	comp    Comparator
	forward bool
}

func (h *iteratorHeap) Len() int      { return len(h.iters) }
func (h *iteratorHeap) Swap(i, j int) { h.iters[i], h.iters[j] = h.iters[j], h.iters[i] }

func (h *iteratorHeap) Less(i, j int) bool {
	val := h.comp.Compare(h.iters[i].iter.Key(), h.iters[j].iter.Key())
	if h.forward {
		if val < 0 {
			return true
		} else if val > 0 {
			return false
		} else if h.iters[i].level < h.iters[j].level {
			return true
		} else {
			return false
		}
	} else {
		if val < 0 {
			return false
		} else if val > 0 {
			return true
		} else if h.iters[i].level < h.iters[j].level {
			return true
		} else {
			return false
		}
	}
}

func (h *iteratorHeap) Push(val interface{}) {
	h.iters = append(h.iters, val.(IteratorPair))
}

func (h *iteratorHeap) Pop() interface{} {
	last := len(h.iters) - 1
	ret := h.iters[last]
	h.iters = h.iters[:last]
	return ret
}

type HeapIterator struct {
	iteratorHeap
	fullSet []Iterator
}

func MakeHeapIterator(iters []Iterator, comp Comparator) Iterator {
	// Only put valid iterators into heap.
	tmp := make([]IteratorPair, 0, len(iters))
	for idx, it := range iters {
		if it.Valid() {
			pair := IteratorPair{iter: it, level: idx}
			tmp = append(tmp, pair)
		}
	}

	ret := &HeapIterator{
		iteratorHeap: iteratorHeap{iters: tmp, comp: comp, forward: true},
		fullSet:      iters,
	}

	heap.Init(ret)

	return ret
}

func (hi *HeapIterator) Valid() bool {
	return len(hi.iters) > 0
}

func (hi *HeapIterator) SeekToFirst() {
	tmp := hi.iters[:0]
	for idx, it := range hi.fullSet {
		it.SeekToFirst()
		if it.Valid() {
			pair := IteratorPair{iter: it, level: idx}
			tmp = append(tmp, pair)
		}
	}

	hi.iters = tmp
	hi.forward = true
	heap.Init(hi)
}

func (hi *HeapIterator) SeekToLast() {
	tmp := hi.iters[:0]
	for idx, it := range hi.fullSet {
		it.SeekToLast()
		if it.Valid() {
			pair := IteratorPair{iter: it, level: idx}
			tmp = append(tmp, pair)
		}
	}

	hi.iters = tmp
	hi.forward = false
	heap.Init(hi)
}

func (hi *HeapIterator) Seek(key []byte) {
	tmp := hi.iters[:0]
	for idx, it := range hi.fullSet {
		it.Seek(key)
		if it.Valid() {
			pair := IteratorPair{iter: it, level: idx}
			tmp = append(tmp, pair)
		}
	}

	hi.iters = tmp
	hi.forward = true
	heap.Init(hi)
}

func (hi *HeapIterator) Next() {
	prevKey := hi.Key()
	if !hi.forward {
		// Iterator through all available iterators and find their corresponding
		// next element.
		list := hi.iters[:0]
		for idx, it := range hi.fullSet {
			it.Seek(prevKey)
			if it.Valid() {
				if hi.comp.Compare(prevKey, it.Key()) == 0 {
					it.Next()
				}
				if it.Valid() {
					pair := IteratorPair{iter: it, level: idx}
					list = append(list, pair)
				}
			}
		}

		hi.iters = list
		hi.forward = true

		heap.Init(hi)
	}

	// Skip same key in higher levels
	for hi.Valid() && hi.comp.Compare(hi.Key(), prevKey) == 0 {
		tmp := heap.Pop(hi).(IteratorPair)
		tmp.iter.Next()
		if tmp.iter.Valid() {
			heap.Push(hi, tmp)
		}
	}
}

func (hi *HeapIterator) Prev() {
	prevKey := hi.Key()
	if hi.forward {
		// Iterator through all available iterators and find their corresponding
		// next element.
		list := hi.iters[:0]
		for idx, it := range hi.fullSet {
			it.Seek(prevKey)
			if it.Valid() {
				it.Prev()
				if it.Valid() {
					pair := IteratorPair{iter: it, level: idx}
					list = append(list, pair)
				}
			} else {
				it.SeekToLast()
				if it.Valid() {
					pair := IteratorPair{iter: it, level: idx}
					list = append(list, pair)
				}
			}
		}

		hi.iters = list
		hi.forward = false

		heap.Init(hi)
	}

	// Skip same key in higher levels
	for hi.Valid() && hi.comp.Compare(hi.Key(), prevKey) == 0 {
		tmp := heap.Pop(hi).(IteratorPair)
		tmp.iter.Prev()
		if tmp.iter.Valid() {
			heap.Push(hi, tmp)
		}
	}
}

func (hi *HeapIterator) Key() []byte {
	return hi.iters[0].iter.Key()
}

func (hi *HeapIterator) Value() []byte {
	return hi.iters[0].iter.Value()
}
