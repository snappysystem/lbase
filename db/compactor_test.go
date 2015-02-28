package db

import (
	"os"
	"testing"
)

func TestL0Compaction(t *testing.T) {
	path := "/tmp/TestL0Compaction"
	os.RemoveAll(path)
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		t.Error("Fails to create testing dir")
	}

	opt := DbOption{
		path:         path,
		env:          MakeNativeEnv(),
		comp:         ByteOrder(0),
		numTblCache:  4,
		minLogSize:   64,
		maxL0Levels:  4,
		minTableSize: 4 * 1024 * 1024,
	}

	db := MakeDb(opt)
	if db == nil {
		t.Error("Fails to create a DB!")
	}

	bigVal := make([]byte, 64)
	for idx, _ := range bigVal {
		bigVal[idx] = 'a'
	}

	var wopt WriteOptions
	var ropt ReadOptions

	db.Put(wopt, []byte("hello"), []byte("world"))
	status, finish := db.PutMore(wopt, []byte("second"), bigVal)

	if !status.Ok() {
		t.Error("Fails to put an item!")
	}

	// Confirm that L0 compaction happens.
	for v := range finish {
		if v != true {
			t.Error("Does not finish compaction successfully")
		}
	}

	var val []byte
	val, status = db.Get(ropt, []byte("hello"))
	if !status.Ok() || string(val) != "world" {
		t.Error("Fails to get a key")
	}
}
