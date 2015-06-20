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
	"bytes"
	"encoding/binary"
	"lbase/db"
	"log"
)

const (
	// Estimate of an average log item's size.
	AvgItemSize = 200
)

type PendingQueueOptions struct {
	QueuePath      string
	QueueKeyPrefix string
}

type PendingQueue struct {
	opts     PendingQueueOptions
	db       db.Db
	lastSeq  int64
	firstSeq int64
	rdOpts   db.ReadOptions
	wrOpts   db.WriteOptions
}

func GetQueueKey(prefix string, seq int64) []byte {
	keyBuf := bytes.NewBufferString(prefix)
	binary.Write(keyBuf, binary.BigEndian, seq)
	return keyBuf.Bytes()
}

func ParseQueueKey(prefix string, qkey []byte) int64 {
	var seq int64
	sz := len(prefix)
	buf := bytes.NewBuffer(qkey[sz:])
	binary.Read(buf, binary.BigEndian, &seq)
	return seq
}

func NewPendingQueue(opts *PendingQueueOptions) *PendingQueue {
	dbopts := db.NewDbOptions()
	dbopts.SetCreateIfMissing(1)

	store, openError := db.OpenDb(dbopts, opts.QueuePath)
	if openError != nil {
		return nil
	}

	return &PendingQueue{
		opts:   *opts,
		db:     store,
		wrOpts: db.NewWriteOptions(),
		rdOpts: db.NewReadOptions(),
	}
}

func (q *PendingQueue) Close() {
	q.db.Close()
}

func (q *PendingQueue) GetLastSequence() int64 {
	if q.lastSeq != 0 {
		return q.lastSeq
	}

	iter := q.db.CreateIterator(q.rdOpts)
	defer iter.Destroy()

	iter.SeekToLast()
	if iter.Valid() {
		qk := iter.Key()
		q.lastSeq = ParseQueueKey(q.opts.QueueKeyPrefix, qk)
	}

	return q.lastSeq
}

func (q *PendingQueue) GetFirstSequence() int64 {
	if q.firstSeq != 0 {
		return q.firstSeq
	}

	iter := q.db.CreateIterator(q.rdOpts)
	defer iter.Destroy()

	iter.SeekToFirst()
	if iter.Valid() {
		qk := iter.Key()
		q.firstSeq = ParseQueueKey(q.opts.QueueKeyPrefix, qk)
	}

	return q.firstSeq
}

func (q *PendingQueue) Put(data []byte) {
	lastSeq := q.GetLastSequence()
	lastSeq++
	q.lastSeq = lastSeq
	qk := GetQueueKey(q.opts.QueueKeyPrefix, lastSeq)

	err := q.db.Put(q.wrOpts, qk, data)
	if err != nil {
		log.Fatal("Fails to write initial data: ", err)
	}
}

func (q *PendingQueue) GetN(seq int64, n int) (data [][]byte, startSeq int64) {
	iter := q.db.CreateIterator(q.rdOpts)
	defer iter.Destroy()

	key := GetQueueKey(q.opts.QueueKeyPrefix, seq)
	iter.Seek(key)

	if !iter.Valid() {
		return
	}

	if bytes.Compare(iter.Key(), key) != 0 {
		log.Panic("Cannot find starting key!")
	}

	startSeq = seq
	for n > 0 && iter.Valid() {
		val := iter.Value()
		data = append(data, val)

		iter.Next()
		n--
	}
	return
}

// Trim pending records up to sequence number @endSeq.
func (q *PendingQueue) Trim(endSeq int64) {
	firstSeq := q.GetFirstSequence()
	lastSeq := q.GetLastSequence()

	if endSeq < lastSeq {
		lastSeq = endSeq
	}

	batch := db.NewWriteBatch()
	defer batch.Destroy()

	for seq := firstSeq; seq < lastSeq; seq++ {
		key := GetQueueKey(q.opts.QueueKeyPrefix, seq)
		batch.Delete(key)
	}

	q.firstSeq = 0
	err := q.db.Write(q.wrOpts, batch)
	if err != nil {
		log.Fatal("Fails to write a batch: ", err)
	}
}
