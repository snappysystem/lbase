/*
Copyright (c) 2015, snappysystem
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
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
