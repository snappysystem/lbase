/*
Copyright (c) 2015, snappysystem
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package server

import (
	"os"
	"testing"
)

func TestEditQueueInsertAndTrim(t *testing.T) {
	name := "EditQueueInsertAndTrim"
	root := "/tmp/test" + name

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	opts := EditQueueOptions{
		QueuePath:      root,
		QueueKeyPrefix: name,
	}
	queue := NewEditQueue(&opts)
	defer queue.Close()

	items := []string{
		"hello",
		"world",
		"pending",
		"queue",
	}

	for _, str := range items {
		queue.AppendEdit([]byte(str))
	}

	res, _ := queue.GetN(1, len(items))
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

func TestEditQueueInsertAndTrimAfterRestart(t *testing.T) {
	name := "EditQueueInsertAndTrimAfterRestart"
	root := "/tmp/test" + name

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	opts := EditQueueOptions{
		QueuePath:      root,
		QueueKeyPrefix: name,
	}
	queue := NewEditQueue(&opts)

	items := []string{
		"hello",
		"world",
		"pending",
		"queue",
	}

	for _, str := range items {
		queue.AppendEdit([]byte(str))
	}

	queue.Close()

	queue = NewEditQueue(&opts)
	res, _ := queue.GetN(1, len(items))
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

	queue = NewEditQueue(&opts)
	if queue.GetFirstSequence() != int64(len(items)) {
		t.Error("Incorrect first sequence")
	}

	if queue.GetLastSequence() != int64(len(items)) {
		t.Error("Incorrect last sequence")
	}

	queue.Close()
}

func TestEditQueueInsertTrimAndInsertAgain(t *testing.T) {
	name := "EditQueueInsertTrimAndInsertAgain"
	root := "/tmp/test" + name

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	opts := EditQueueOptions{
		QueuePath:      root,
		QueueKeyPrefix: name,
	}
	queue := NewEditQueue(&opts)
	defer queue.Close()

	items := []string{
		"hello",
		"world",
		"pending",
		"queue",
	}

	for _, str := range items {
		queue.AppendEdit([]byte(str))
	}

	queue.Trim(int64(len(items)))

	items = []string{
		"2hello",
		"2world",
		"2pending",
		"2queue",
	}

	for _, str := range items {
		queue.AppendEdit([]byte(str))
	}

	if queue.GetFirstSequence() != int64(len(items)) {
		t.Error("Incorrect first sequence", queue.GetFirstSequence())
	}

	if queue.GetLastSequence() != int64(2*len(items)) {
		t.Error("Incorrect last sequence")
	}
}
