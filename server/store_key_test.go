package server

import (
	"testing"
)

func TestStoreKey(t *testing.T) {
	rawKey := "hello"
	ver := int64(1004)

	sKey := NewStoreKey([]byte(rawKey), ver)
	retKey, retVer := ParseStoreKey(sKey)
	if retKey == nil {
		t.Error("Fails to parse store key!")
	}
	if string(retKey) != rawKey || retVer != ver {
		t.Error("Parsed value mismatch!")
	}
}
