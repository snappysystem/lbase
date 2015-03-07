package db

import (
	"fmt"
	"os"
	"testing"
)

func TestL0Compaction(t *testing.T) {
	path := "/tmp/compactor_test/TestL0Compaction"
	os.RemoveAll(path)
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		t.Error("Fails to create testing dir")
	}

	opt := DbOption{
		path:         path,
		env:          MakeNativeEnv(),
		comp:         ByteOrder(0),
		numTblCache:  8,
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

	for i := 1000; i < 1002; i++ {
		key := fmt.Sprintf("%d", i)
		status, finish := db.PutMore(wopt, []byte(key), bigVal)

		if !status.Ok() {
			t.Error("Fails to put an item!")
		}

		// Wait until L0 compaction completes.
		for v := range finish {
			if v != true {
				t.Error("Does not finish compaction successfully")
			}
		}
	}

	val, status := db.Get(ropt, []byte("hello"))
	if !status.Ok() || string(val) != "world" {
		t.Error("Fails to get a key")
	}
}
