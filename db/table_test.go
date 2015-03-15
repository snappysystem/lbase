package db

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestBuildTableAndIterate(t *testing.T) {
	root := "/tmp/table_test/testBuildTableAndIterate"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	// create a table builder
	fname := strings.Join([]string{root, "sstfile.nsst"}, "/")
	f := MakeLocalWritableFile(fname)
	if f == nil {
		t.Error("Fails to create a new file")
	}

	b := MakeTableBuilder(f, 2*1024*1024)

	// build a table
	for i := 10000; i < 10256; i++ {
		key := []byte(fmt.Sprintf("%d", i))
		b.Add(key, key)
	}

	order := ByteOrder(0)
	res := b.Finalize(order)

	if res == nil {
		t.Error("Fails to get a table object")
	}

	// verify that data is correct
	iter := res.NewIterator()
	if iter == nil {
		t.Error("fails to get an iterator")
	}

	iter.SeekToFirst()

	for i := 10000; i < 10256; i++ {
		if !iter.Valid() {
			t.Error("Premature at the end")
		}

		key := string(iter.Key())
		val, err := strconv.Atoi(key)
		if err != nil {
			t.Error("fails convert string to integer")
		}
		if val != i {
			t.Error("key mismatch ", val, " expect ", i)
		}

		iter.Next()
	}

	if iter.Valid() {
		t.Error("iterator passes the end")
	}
}

func TestBuildTableAndMoveBackward(t *testing.T) {
	root := "/tmp/table_test/testBuildTableAndMoveBackward"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	// create a table builder
	fname := strings.Join([]string{root, "sstfile.nsst"}, "/")
	f := MakeLocalWritableFile(fname)
	if f == nil {
		t.Error("Fails to create a new file")
	}

	b := MakeTableBuilder(f, 2*1024*1024)

	// build a table
	for i := 10000; i < 10256; i++ {
		key := []byte(fmt.Sprintf("%d", i*10))
		b.Add(key, key)
	}

	order := ByteOrder(0)
	res := b.Finalize(order)

	if res == nil {
		t.Error("Fails to get a table object")
	}

	// verify that data is correct
	iter := res.NewIterator()
	if iter == nil {
		t.Error("fails to get an iterator")
	}

	iter.SeekToLast()
	if !iter.Valid() {
		t.Error("Last is not valid!")
	}

	for i := 10255; i >= 10000; i-- {
		key := fmt.Sprintf("%d", i*10)
		if !iter.Valid() || string(iter.Value()) != key {
			t.Error("Fails to find matching value")
		}
		iter.Prev()
	}

	iter.Close()
}

func TestBuildTableAndZigZagScan(t *testing.T) {
	root := "/tmp/table_test/testBuildTableAndZigZagScan"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	// create a table builder
	fname := strings.Join([]string{root, "sstfile.nsst"}, "/")
	f := MakeLocalWritableFile(fname)
	if f == nil {
		t.Error("Fails to create a new file")
	}

	b := MakeTableBuilder(f, 2*1024*1024)

	// build a table
	for i := 10000; i < 10006; i++ {
		key := []byte(fmt.Sprintf("%d", i))
		b.Add(key, key)
	}

	order := ByteOrder(0)
	res := b.Finalize(order)

	if res == nil {
		t.Error("Fails to get a table object")
	}

	// verify that data is correct
	iter := res.NewIterator()
	if iter == nil {
		t.Error("fails to get an iterator")
	}

	iter.SeekToFirst()

	for i := 10000; i < 10006; i++ {
		if !iter.Valid() {
			t.Error("Premature at the end")
		}

		key := string(iter.Key())
		val, err := strconv.Atoi(key)
		if err != nil {
			t.Error("fails convert string to integer", key)
		}
		if val != i {
			t.Error("key mismatch ", val, " expect ", i)
		}

		iter.Prev()
		if iter.Valid() {
			iter.Next()
		} else {
			iter.SeekToFirst()
		}

		iter.Next()
	}

	if iter.Valid() {
		t.Error("iterator passes the end")
	}
}

func TestBuildTableAndSeek(t *testing.T) {
	root := "/tmp/table_test/testBuildTableAndIterate"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	// create a table builder
	fname := strings.Join([]string{root, "sstfile.nsst"}, "/")
	f := MakeLocalWritableFile(fname)
	if f == nil {
		t.Error("Fails to create a new file")
	}

	b := MakeTableBuilder(f, 2*1024*1024)

	// build a table
	for i := 10000; i < 10256; i++ {
		key := []byte(fmt.Sprintf("%d", i*10))
		b.Add(key, key)
	}

	order := ByteOrder(0)
	res := b.Finalize(order)

	if res == nil {
		t.Error("Fails to get a table object")
	}

	// verify that data is correct
	iter := res.NewIterator()
	if iter == nil {
		t.Error("fails to get an iterator")
	}

	iter.Seek([]byte("100020"))
	if !iter.Valid() || string(iter.Key()) != "100020" {
		t.Error("Fails to seek to exact location")
	}

	iter.Seek([]byte("100082"))
	if !iter.Valid() || string(iter.Key()) != "100090" {
		t.Error("Fails to seek to closest location")
	}

	iter.Close()
}

func TestBuildTableAndRecover(t *testing.T) {
	root := "/tmp/table_test/testBuildTableAndRecover"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	fname := strings.Join([]string{root, "sstfile"}, "/")

	{
		// create a table builder
		f := MakeLocalWritableFile(fname)
		if f == nil {
			t.Error("Fails to create a new file")
		}

		b := MakeTableBuilder(f, 2*1024*1024)

		// build a table
		for i := 10000; i < 10256; i++ {
			key := []byte(fmt.Sprintf("%d", i))
			b.Add(key, key)
		}

		order := ByteOrder(0)
		res := b.Finalize(order)

		if res == nil {
			t.Error("Fails to get a table object")
		}
	}

	// verify that data is correct
	{
		f := MakeLocalSequentialFile(fname)
		if f == nil {
			t.Error("Fails to open table file for read")
		}

		defer f.Close()

		// get file size
		var fsize int64
		{
			fobj, err := os.Open(fname)
			if err != nil {
				t.Error("Fails to open a file")
			}

			fi, e2 := fobj.Stat()
			if e2 != nil {
				t.Error("Fails to stat a file")
			}

			fsize = fi.Size()
			fobj.Close()
		}

		buf := make([]byte, fsize)
		order := ByteOrder(0)

		table := RecoverTable(f, buf, order)
		if table == nil {
			t.Error("Fails to recover from a table file")
		}

		iter := table.NewIterator()
		if iter == nil {
			t.Error("fails to get an iterator")
		}

		iter.SeekToFirst()

		for i := 10000; i < 10256; i++ {
			if !iter.Valid() {
				t.Error("Premature at the end")
			}

			key := string(iter.Key())
			val, err := strconv.Atoi(key)
			if err != nil {
				t.Error("fails convert string to integer")
			}
			if val != i {
				t.Error("key mismatch ", val, " expect ", i)
			}

			iter.Next()
		}

		if iter.Valid() {
			t.Error("iterator passes the end")
		}
	}
}
