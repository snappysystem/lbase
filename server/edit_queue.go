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

// A EditQueue stores client's update requests before raft leader
// collect them. Each member of the quorum has a pending queue. Once
// the records have been collected by the leader and committed to
// the quorum, they can be removed safely from the pending queue.

type EditQueueOptions struct {
	QueuePath      string
	QueueKeyPrefix string
}

type EditQueue struct {
	opts     EditQueueOptions
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

func NewEditQueue(opts *EditQueueOptions) *EditQueue {
	dbopts := db.NewDbOptions()
	dbopts.SetCreateIfMissing(1)

	store, openError := db.OpenDb(dbopts, opts.QueuePath)
	if openError != nil {
		return nil
	}

	return &EditQueue{
		opts:   *opts,
		db:     store,
		wrOpts: db.NewWriteOptions(),
		rdOpts: db.NewReadOptions(),
	}
}

func (q *EditQueue) Close() {
	q.db.Close()
}

func (q *EditQueue) GetLastSequence() int64 {
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

func (q *EditQueue) GetFirstSequence() int64 {
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

func (q *EditQueue) AppendEdit(data []byte) {
	lastSeq := q.GetLastSequence()
	lastSeq++
	q.lastSeq = lastSeq
	qk := GetQueueKey(q.opts.QueueKeyPrefix, lastSeq)

	err := q.db.Put(q.wrOpts, qk, data)
	if err != nil {
		log.Fatal("Fails to write initial data: ", err)
	}
}

func (q *EditQueue) GetN(seq int64, n int) (data [][]byte, startSeq int64) {
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
func (q *EditQueue) Trim(endSeq int64) {
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
