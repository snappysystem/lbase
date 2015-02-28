package db

import (
	"os"
	"testing"
)

func TestSimplePut(t *testing.T) {
	path := "/tmp/TestSimplePut"
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
		minLogSize:   4096,
		maxL0Levels:  4,
		minTableSize: 4 * 1024 * 1024,
	}

	db := MakeDb(opt)
	if db == nil {
		t.Error("Fails to create a DB!")
	}

	data := map[string]string{
		"hello":  "world",
		"34567":  "dffafa",
		"others": "27182",
	}

	var wopt WriteOptions
	var ropt ReadOptions

	for k, v := range data {
		status := db.Put(wopt, []byte(k), []byte(v))
		if !status.Ok() {
			t.Error("Fails to put a key")
		}
	}

	for k, v := range data {
		val, status := db.Get(ropt, []byte(k))
		if !status.Ok() || string(val) != v {
			t.Error("Fails to get a key")
		}
	}
}
