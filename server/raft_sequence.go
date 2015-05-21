package server

import (
	"sort"
)

type RaftSequence struct {
	Index int64
	Term  int64
}

type RaftSequenceList []RaftSequence

// Part of "sort.Interface".
func (s RaftSequenceList) Len() int {
	return len(s)
}

// Part of "sort.Interface".
func (s RaftSequenceList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Part of "sort.Interface".
// Comparing two index.
func (s RaftSequenceList) Less(i, j int) bool {
	return s.LessValue(i, &(s[j]))
}

// Comparing one index with one value.
func (s RaftSequenceList) LessValue(i int, seq *RaftSequence) bool {
	if s[i].Term < seq.Term {
		return true
	} else if s[i].Term > seq.Term {
		return false
	} else {
		return s[i].Index < seq.Index
	}
}

// Binary search on a sequence list, return the index in the slice.
func (s RaftSequenceList) Search(seq RaftSequence) int {
	return sort.Search(len(s), func(i int) bool { return !s.LessValue(i, &seq) })
}
