package db

// Implement a skip list, a dynamically sorted data structure.
// A skiplist enables concurrent insertion and query.
//
// A skiplist consists of multiple levels of single linked list.
// Nodes in last level are leaves, while nodes in other levels
// are called pointer nodes which has a pointer to the corresponding
// node in next level.
//
// The nodes in each level is sorted by the key fields. The higher
// level has expenentially less number of nodes than that of lower
// levels

import (
	"math/rand"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"
)

// A random generator to decide that how many levels that a new key
// should be inserted into.
var (
	levels      = [...]int{8, 64, 512, 4096, 32768, 262144}
	levelsSlice = levels[:]
)

const maxLevel = len(levels)

type randomGenerator struct {
	r *rand.Rand
}

func makeRandomGenerator() *randomGenerator {
	x := randomGenerator{}
	seed := time.Now().UTC().UnixNano()
	x.r = rand.New(rand.NewSource(seed))
	return &x
}

func (x *randomGenerator) get() int {
	max := levels[maxLevel-1]
	val := x.r.Intn(max)
	// i will always be less than @maxLevel
	i := sort.SearchInts(levelsSlice, val)
	return (maxLevel - i - 1)
}

// general interface for a skiplistNode in skip list
type skiplistNode interface {
	getKey() []byte
	getNext() skiplistNode
	getChild() skiplistNode
	setKey(key []byte)
	setNext(next skiplistNode)
	setChild(child skiplistNode)
}

// A leaf node in skip list
type skiplistLeafNode struct {
	key   []byte
	value []byte
	next  *skiplistLeafNode
}

func (a *skiplistLeafNode) getKey() []byte {
	return a.key
}

func (a *skiplistLeafNode) getNext() skiplistNode {
	if a.next != nil {
		return a.next
	} else {
		return nil
	}
}

func (a *skiplistLeafNode) getChild() skiplistNode {
	return nil
}

func (a *skiplistLeafNode) setKey(key []byte) {
	a.key = key
}

func (a *skiplistLeafNode) setNext(next skiplistNode) {
	var val *skiplistLeafNode
	if next != nil {
		val = next.(*skiplistLeafNode)
	}
	dst := (*unsafe.Pointer)(unsafe.Pointer(&a.next))
	atomic.StorePointer(dst, unsafe.Pointer(val))
}

func (a *skiplistLeafNode) setChild(child skiplistNode) {
	panic("should not set child on leaf node")
}

// an internal (non-leaf) node in skiplist
type skiplistPointerNode struct {
	key   []byte
	next  *skiplistPointerNode
	child skiplistNode
}

func (a *skiplistPointerNode) getKey() []byte {
	return a.key
}

func (a *skiplistPointerNode) getNext() skiplistNode {
	if a.next != nil {
		return a.next
	} else {
		return nil
	}
}

func (a *skiplistPointerNode) getChild() skiplistNode {
	return a.child
}

func (a *skiplistPointerNode) setKey(key []byte) {
	a.key = key
}

func (a *skiplistPointerNode) setNext(next skiplistNode) {
	var val *skiplistPointerNode
	if next != nil {
		val = next.(*skiplistPointerNode)
	}
	dst := (*unsafe.Pointer)(unsafe.Pointer(&a.next))
	atomic.StorePointer(dst, unsafe.Pointer(val))
}

func (a *skiplistPointerNode) setChild(child skiplistNode) {
	a.child = child
}

type Skiplist struct {
	levels   []skiplistNode
	gen      *randomGenerator
	order    Comparator
	numNodes int
}

// Create a new skiplist. It can take up to 1 parameters:
// First optional parameter (Comparator): the customized comparator
func MakeSkiplist(args ...interface{}) *Skiplist {
	ret := Skiplist{}

	switch len(args) {
	case 0:
		ret.order = ByteOrder(0)
	case 1:
		ret.order = args[0].(Comparator)
	default:
		panic("args is either 0 or 1")
	}

	ret.levels = make([]skiplistNode, maxLevel+1)
	ret.gen = makeRandomGenerator()
	ret.numNodes = 0
	return &ret
}

// Insert a key value pair into skip list.
func (a *Skiplist) Put(key []byte, val []byte) {
	prevList, found := a.trace(key)
	if found {
		leaf := prevList[0].(*skiplistLeafNode)
		leaf.value = val
		return
	}

	height := a.gen.get() + 1
	var child skiplistNode

	for i := 0; i < height; i++ {
		var newNode skiplistNode
		if i == 0 {
			newNode = &skiplistLeafNode{value: val}
		} else {
			newNode = &skiplistPointerNode{}
		}

		newNode.setKey(key)

		if prevList[i] != nil {
			newNode.setNext(prevList[i].getNext())
			prevList[i].setNext(newNode)
		} else {
			newNode.setNext(a.levels[i])
			a.levels[i] = newNode
		}

		if child != nil {
			newNode.setChild(child)
		}

		child = newNode
	}
}

// Look up a key in the skiplist. Return the corresponding value and true
// if the key is in the skiplist. Otherwise return an empty slice and
// false
func (a *Skiplist) Get(key []byte) (value []byte, ok bool) {
	prevList, ok := a.trace(key)
	if ok {
		leaf := prevList[0].(*skiplistLeafNode)
		value, ok = leaf.value, true
	} else {
		ok = false
	}
	return
}

func (a *Skiplist) NewIterator(opt *ReadOptions) Iterator {
	return makeSkiplistIter(a)
}

// Find out nodes in all levels that point a key either before @key or
// exactly point to @key. Return true if @key is in the skip list,
// otherwise false
func (a *Skiplist) trace(key []byte) (ret []skiplistNode, found bool) {
	numLevels := len(a.levels)
	ret = make([]skiplistNode, numLevels)
	var prev skiplistNode

	for cur, i := a.levels[numLevels-1], numLevels-1; i >= 0; {
		if cur == nil {
			i--
			if i >= 0 {
				cur = a.levels[i]
			} else {
				break
			}
			continue
		}

		switch a.order.Compare(cur.getKey(), key) {
		case -1:
			prev = cur
			cur = cur.getNext()
			if cur == nil {
				ret[i] = prev
				i--
				cur = prev.getChild()
				prev = nil
			}
		case 0:
			found = true
			ret[i] = cur
			i--
			cur = cur.getChild()
			prev = nil
		case 1:
			ret[i] = prev
			i--
			if prev != nil {
				cur = prev.getChild()
				prev = nil
			} else if i >= 0 {
				cur = a.levels[i]
			}
		default:
			panic("Invaid comparison value")
		}
	}

	return
}

func (a *Skiplist) traceBackward(key []byte) []skiplistNode {
	numLevels := len(a.levels)
	ret := make([]skiplistNode, numLevels)
	var prev skiplistNode

	for cur, i := a.levels[numLevels-1], numLevels-1; i >= 0; {
		if cur == nil {
			i--
			if i >= 0 {
				cur = a.levels[i]
			} else {
				break
			}
			continue
		}

		switch a.order.Compare(cur.getKey(), key) {
		case -1:
			prev = cur
			cur = cur.getNext()
			if cur == nil {
				ret[i] = prev
				i--
				cur = prev.getChild()
				prev = nil
			}
		case 0:
			fallthrough
		case 1:
			ret[i] = prev
			i--
			if prev != nil {
				cur = prev.getChild()
				prev = nil
			} else if i >= 0 {
				cur = a.levels[i]
			}
		default:
			panic("Invaid comparison value")
		}
	}

	return ret
}

func (a *Skiplist) locateLast() skiplistNode {
	numLevels := len(a.levels)
	ret := make([]skiplistNode, numLevels)
	var prev skiplistNode

	for cur, i := a.levels[numLevels-1], numLevels-1; i >= 0; {
		if cur == nil {
			i--
			if i >= 0 {
				cur = a.levels[i]
			} else {
				break
			}
			continue
		}

		switch {
		case cur.getNext() != nil:
			prev = cur
			cur = cur.getNext()
			if cur == nil {
				ret[i] = prev
				i--
				cur = prev.getChild()
				prev = nil
			}
		default:
			ret[i] = cur
			i--
			if prev != nil {
				cur = prev.getChild()
				prev = nil
			} else if i >= 0 {
				cur = a.levels[i]
			}
		}
	}

	return ret[0]
}

// Iterator class for skiplist
type skiplistIter struct {
	slist *Skiplist
	cur   skiplistNode
}

func makeSkiplistIter(s *Skiplist) *skiplistIter {
	ret := &skiplistIter{}
	ret.slist = s
	return ret
}

func (a *skiplistIter) Valid() bool {
	return a.cur != nil
}

func (a *skiplistIter) SeekToFirst() {
	a.cur = a.slist.levels[0]
}

func (a *skiplistIter) SeekToLast() {
	a.cur = a.slist.locateLast()
}

func (a *skiplistIter) Seek(key []byte) {
	traces, match := a.slist.trace(key)
	if match {
		a.cur = traces[0]
	} else if traces[0] != nil {
		a.cur = traces[0].getNext()
	} else {
		// It is possible that the skiplist is not empty,
		// but the key we are looking for is at the very
		// beginning. In this case, search linearly from start.
		for a.SeekToFirst(); a.Valid() && a.slist.order.Compare(a.Key(), key) < 0; a.Next() {
		}
	}
}

func (a *skiplistIter) Next() {
	a.cur = a.cur.getNext()
}

func (a *skiplistIter) Prev() {
	key := a.cur.getKey()
	a.cur = a.slist.traceBackward(key)[0]
}

func (a *skiplistIter) Key() []byte {
	return a.cur.getKey()
}

func (a *skiplistIter) Value() []byte {
	leaf := a.cur.(*skiplistLeafNode)
	return leaf.value
}

func (a *skiplistIter) Close() {
}
