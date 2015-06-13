package server

import (
	"os"
	"testing"
)

func TestPendingQueueInsertAndTrim(t *testing.T) {
	name := "PendingQueueInsertAndTrim"
	root := "/tmp/test" + name

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	opts := PendingQueueOptions{
		QueuePath:      root,
		QueueKeyPrefix: name,
	}
	queue := NewPendingQueue(&opts)
	defer queue.Close()

	items := []string{
		"hello",
		"world",
		"pending",
		"queue",
	}

	for _, str := range items {
		queue.Put([]byte(str))
	}

	res, _ := queue.GetN(len(items))
	if len(res) != len(items) {
		t.Error("Fails to get items")
	}

	for idx, data := range res {
		if string(data) != items[idx] {
			t.Error("item mismatch")
		}
	}

	if queue.GetFirstSequence() != int64(1) {
		t.Error("Incorrect first sequence")
	}

	if queue.GetLastSequence() != int64(len(items)) {
		t.Error("Incorrect last sequence")
	}

	queue.Trim(int64(len(items)))

	if queue.GetFirstSequence() != int64(len(items)) {
		t.Error("Incorrect first sequence", queue.GetFirstSequence())
	}

	if queue.GetLastSequence() != int64(len(items)) {
		t.Error("Incorrect last sequence")
	}
}

func TestPendingQueueInsertAndTrimAfterRestart(t *testing.T) {
	name := "PendingQueueInsertAndTrimAfterRestart"
	root := "/tmp/test" + name

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	opts := PendingQueueOptions{
		QueuePath:      root,
		QueueKeyPrefix: name,
	}
	queue := NewPendingQueue(&opts)

	items := []string{
		"hello",
		"world",
		"pending",
		"queue",
	}

	for _, str := range items {
		queue.Put([]byte(str))
	}

	queue.Close()

	queue = NewPendingQueue(&opts)
	res, _ := queue.GetN(len(items))
	if len(res) != len(items) {
		t.Error("Fails to get items")
	}

	for idx, data := range res {
		if string(data) != items[idx] {
			t.Error("item mismatch")
		}
	}

	if queue.GetFirstSequence() != int64(1) {
		t.Error("Incorrect first sequence")
	}

	if queue.GetLastSequence() != int64(len(items)) {
		t.Error("Incorrect last sequence")
	}

	queue.Trim(int64(len(items)))

	queue.Close()

	queue = NewPendingQueue(&opts)
	if queue.GetFirstSequence() != int64(len(items)) {
		t.Error("Incorrect first sequence")
	}

	if queue.GetLastSequence() != int64(len(items)) {
		t.Error("Incorrect last sequence")
	}

	queue.Close()
}

func TestPendingQueueInsertTrimAndInsertAgain(t *testing.T) {
	name := "PendingQueueInsertTrimAndInsertAgain"
	root := "/tmp/test" + name

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	opts := PendingQueueOptions{
		QueuePath:      root,
		QueueKeyPrefix: name,
	}
	queue := NewPendingQueue(&opts)
	defer queue.Close()

	items := []string{
		"hello",
		"world",
		"pending",
		"queue",
	}

	for _, str := range items {
		queue.Put([]byte(str))
	}

	queue.Trim(int64(len(items)))

	items = []string{
		"2hello",
		"2world",
		"2pending",
		"2queue",
	}

	for _, str := range items {
		queue.Put([]byte(str))
	}

	if queue.GetFirstSequence() != int64(len(items)) {
		t.Error("Incorrect first sequence", queue.GetFirstSequence())
	}

	if queue.GetLastSequence() != int64(2*len(items)) {
		t.Error("Incorrect last sequence")
	}
}
