package server

import (
	"bytes"
	"encoding/binary"
)

const (
	kSizeOfInt64 = 8
)

// Convertions between store key and real key.
// A store key is a real key appended with version value.

func NewStoreKey(key []byte, ver int64) []byte {
	keyBuf := bytes.NewBuffer(key)
	binary.Write(keyBuf, binary.BigEndian, ver)
	return keyBuf.Bytes()
}

func ParseStoreKey(storeKey []byte) (key []byte, ver int64) {
	sz := len(storeKey)
	if sz < kSizeOfInt64 {
		return
	}

	buf := bytes.NewBuffer(storeKey[sz-kSizeOfInt64:])
	binary.Read(buf, binary.BigEndian, &ver)
	key = storeKey[:sz-kSizeOfInt64]
	return
}
