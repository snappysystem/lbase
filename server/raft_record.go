package server

import (
	"bytes"
	"encoding/gob"
)

type RaftRecord struct {
	Key   []byte
	Value []byte
}

// Parse a slice to store a raft record.
func NewRaftRecord(msg []byte) (ret *RaftRecord, err error) {
	ret = &RaftRecord{}
	b := bytes.NewBuffer(msg)
	dec := gob.NewDecoder(b)
	err = dec.Decode(ret)
	return
}

// Serialize a raft record into a slice.
func (r *RaftRecord) ToSlice() (res []byte, err error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err = enc.Encode(r)
	if err == nil {
		res = b.Bytes()
	}
	return
}
