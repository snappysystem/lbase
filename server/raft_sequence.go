package server

import (
	"bytes"
	"encoding/binary"
	"sort"
)

type RaftSequence struct {
	Index int64
	Term  int64
}

// Deserialize a sequence from a db key.
func NewRaftSequenceFromKey(key []byte) (s *RaftSequence, err error) {
	b := bytes.NewBuffer(key)
	s = &RaftSequence{}

	err = binary.Read(b, binary.BigEndian, &s.Term)
	if err != nil {
		return
	}

	err = binary.Read(b, binary.BigEndian, &s.Index)
	return
}

// Serialize a sequence value to a db key.
func (s RaftSequence) AsKey() []byte {
	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, s.Term)
	binary.Write(&b, binary.BigEndian, s.Index)
	return b.Bytes()
}

func (s RaftSequence) Less(s2 RaftSequence) bool {
	if s.Term < s2.Term {
		return true
	} else if s.Term > s2.Term {
		return false
	} else {
		return s.Index < s2.Index
	}
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
	return s[i].Less(s[j])
}

// Binary search on a sequence list, return the index in the slice.
func (s RaftSequenceList) Search(seq RaftSequence) int {
	return sort.Search(len(s), func(i int) bool { return !s[i].Less(seq) })
}
