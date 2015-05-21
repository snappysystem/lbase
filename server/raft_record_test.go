package server

import (
	"fmt"
	"testing"
)

func TestRaftRecord(t *testing.T) {
	record := RaftRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
	}

	msg, serializeErr := record.ToSlice()
	if serializeErr != nil {
		t.Error(fmt.Sprintf("%#v\n", serializeErr))
	}

	newRecord, deserializeErr := NewRaftRecord(msg)
	if deserializeErr != nil {
		t.Error(fmt.Sprintf("%#v\n", deserializeErr))
	}

	if string(newRecord.Key) != string(record.Key) {
		t.Error("record does not match with original")
	}
}
