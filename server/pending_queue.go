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
	NumReplicas    int
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

func (q *PendingQueue) GetN(n int) (data [][]byte, startSeq int64) {
	startSeq = q.GetFirstSequence()

	iter := q.db.CreateIterator(q.rdOpts)
	defer iter.Destroy()

	iter.SeekToFirst()
	for n > 0 && iter.Valid() {
		val := iter.Value()
		data = append(data, val)

		iter.Next()
		n--
	}
	return
}

func (q *PendingQueue) Trim(startSeq int64, n int) {
	seq := q.GetFirstSequence()

	// Adjust start sequence.
	diff := seq - startSeq
	intdiff := int(diff)
	if int64(intdiff) != diff {
		log.Panic("Integer overflow!")
	}
	startSeq = seq
	n = n - intdiff

	batch := db.NewWriteBatch()
	defer batch.Destroy()

	for n > 0 {
		key := GetQueueKey(q.opts.QueueKeyPrefix, startSeq)
		batch.Delete(key)

		n--
		startSeq++
	}

	err := q.db.Write(q.wrOpts, batch)
	if err != nil {
		log.Fatal("Fails to write a batch: ", err)
	}
}
