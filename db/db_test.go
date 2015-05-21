package db

import(
	"os"
	"sort"
	"testing"
)

func TestDbCreateSuccess(t *testing.T) {
	path := "/tmp/testDbCreateSuccess"
	os.RemoveAll(path)

	opts := NewDbOptions()
	opts.SetCreateIfMissing(1)

	_,openError := OpenDb(opts, path)
	if openError != nil {
		t.Error("Fails to open db")
	}
}

func TestDbCreateFailure(t *testing.T) {
	path := "/tmp/testDbCreateFailure"
	os.RemoveAll(path)

	opts := NewDbOptions()
	opts.SetCreateIfMissing(0)

	_,openError := OpenDb(opts, path)
	if openError == nil {
		t.Error("Expect to fails to open db")
	}
}

func TestDbPutGet(t *testing.T) {
	path := "/tmp/testDbPutGet"
	os.RemoveAll(path)

	opts := NewDbOptions()
	opts.SetCreateIfMissing(1)

	db,openError := OpenDb(opts, path)
	if openError != nil {
		t.Error("Fails to open db")
	}

	defer db.Close()

	key, value := "hello", "world"

	putError := db.Put(NewWriteOptions(), []byte(key), []byte(value))
	if putError != nil {
		t.Error("Fails to put")
	}

	res,getError := db.Get(NewReadOptions(), []byte(key))
	if getError != nil || string(res) != value {
		t.Error("Fails to get")
	}
}

func TestDbWriteIter(t *testing.T) {
	path := "/tmp/testDbWriteIter"
	os.RemoveAll(path)

	opts := NewDbOptions()
	opts.SetCreateIfMissing(1)

	db,openError := OpenDb(opts, path)
	if openError != nil {
		t.Error("Fails to open db")
	}

	defer db.Close()

	keys := []string{"hello", "world", "goleveldb"}
	batch := NewWriteBatch()
	for _,k := range keys {
		b := []byte(k)
		batch.Put(b, b)
	}

	writeError := db.Write(NewWriteOptions(), batch)
	if writeError != nil {
		t.Error("Fails to multi-write")
	}

	iter := db.CreateIterator(NewReadOptions())
	defer iter.Destroy()

	sort.Strings(keys)

	iter.SeekToFirst()
	if !iter.Valid() || string(iter.Key()) != keys[0] {
		t.Error("Fails to seek to first")
	}

	iter.SeekToLast()
	if !iter.Valid() || string(iter.Key()) != keys[len(keys)-1] {
		t.Error("Fails to seek to first")
	}
}

func TestDbWriteIterViewChange(t *testing.T) {
	path := "/tmp/testDbWriteIter"
	os.RemoveAll(path)

	opts := NewDbOptions()
	opts.SetCreateIfMissing(1)

	db,openError := OpenDb(opts, path)
	if openError != nil {
		t.Error("Fails to open db")
	}

	defer db.Close()

	keys := []string{"hello", "world", "goleveldb"}
	batch := NewWriteBatch()
	for _,k := range keys {
		b := []byte(k)
		batch.Put(b, b)
	}

	writeError := db.Write(NewWriteOptions(), batch)
	if writeError != nil {
		t.Error("Fails to multi-write")
	}

	iter := db.CreateIterator(NewReadOptions())
	defer iter.Destroy()

	sort.Strings(keys)

	iter.SeekToLast()
	if !iter.Valid() || string(iter.Key()) != keys[len(keys)-1] {
		t.Error("Fails to seek to last")
	}

	last := "zebra"
	db.Put(NewWriteOptions(), []byte(last), []byte(last))

	iter.SeekToLast()
	if !iter.Valid() || string(iter.Key()) != keys[len(keys)-1] {
		t.Error("Fails to seek to last")
	}
}
