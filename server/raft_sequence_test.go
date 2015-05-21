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
