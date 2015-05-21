package server

import (
	"fmt"
	"sort"
	"testing"
)

func TestRaftSequenceSort(t *testing.T) {
	list := RaftSequenceList{
		RaftSequence{Index: 34, Term: 25},
		RaftSequence{Index: 32, Term: 25},
		RaftSequence{Index: 103, Term: 12},
	}

	sort.Sort(list)

	expectedIndices := []int64{103, 32, 34}
	for i := 0; i < len(list); i++ {
		if expectedIndices[i] != list[i].Index {
			x, y := expectedIndices[i], list[i].Index
			t.Error(fmt.Sprintf("expect %d, get %d\n", x, y))
		}
	}
}

func TestRaftSequenceSearch(t *testing.T) {
	list := RaftSequenceList{
		RaftSequence{Index: 34, Term: 25},
		RaftSequence{Index: 32, Term: 25},
		RaftSequence{Index: 103, Term: 12},
	}

	sort.Sort(list)
	seq := RaftSequence{Index: 256, Term: 17}
	res := list.Search(seq)
	if res != 1 {
		t.Error(fmt.Sprintf("got index %d\n", res))
	}
}

func TestRaftSequenceSerDeser(t *testing.T) {
	seq := RaftSequence{Term: 206, Index: 1123}
	bytes := seq.AsKey()
	newSeq, err := NewRaftSequenceFromKey(bytes)
	if err != nil {
		t.Error(fmt.Sprintf("%#v\n", err))
	}

	if *newSeq != seq {
		t.Error(fmt.Sprintf("seq mistmatch %#v:%#v\n", newSeq, seq))
	}
}

func TestRaftSequenceOrder(t *testing.T) {
	s1 := RaftSequence{Term: 206, Index: 1123}
	s2 := RaftSequence{Term: 26, Index: 1123}
	b1 := s1.AsKey()
	b2 := s2.AsKey()
	if string(b1) <= string(b2) {
		t.Error("Order is not correct")
	}
}
