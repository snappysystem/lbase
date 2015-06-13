package server

import (
	"os"
	"testing"
)

func TestPendingQueueInsertAndTrim(t *testing.T) {
	root := "/tmp/testPendingQueueInsertAndTrim"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	opts := PendingQueueOptions{
		QueuePath:      root,
		QueueKeyPrefix: "PendingQueueInsertAndTrim",
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

	queue.Trim(int64(1), len(items))

	if queue.GetFirstSequence() != int64(len(items)+1) {
		t.Error("Incorrect first sequence")
	}

	if queue.GetLastSequence() != int64(len(items)) {
		t.Error("Incorrect last sequence")
	}
}
