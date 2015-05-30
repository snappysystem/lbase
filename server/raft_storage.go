package server

import (
	"fmt"
	"lbase/db"
)

type RaftCommitStatus int

const (
	COMMIT_OK RaftCommitStatus = iota
	COMMIT_NOT_MATCH
	COMMIT_NOT_FOUND
	COMMIT_PARSE_ERROR
)

type RaftStorage struct {
	// Raft logs are kept in a separate db.
	log db.Db
	// Stores the committed data.
	store *RegionStore
	// Various options for raft storage.
	opts *RaftOptions
	// Leveldb write options.
	wrOpts db.WriteOptions
	// Leveldb read options
	rdOpts db.ReadOptions
	// Latest sequence number in the log.
	lastRaftSequence *RaftSequence
	// Latest Raft sequence that has been committed.
	lastCommitSequence *RaftSequence
}

func NewRaftStorage(opts *RaftOptions, store *RegionStore) (ret *RaftStorage, err error) {
	// Create a log db if we have not done so yet.
	logopts := db.NewDbOptions()
	logopts.SetCreateIfMissing(1)

	log, openError := db.OpenDb(logopts, opts.LogDbRoot)
	if openError != nil {
		err = openError
		return
	}

	ret = &RaftStorage{
		log:    log,
		store:  store,
		opts:   opts,
		wrOpts: db.NewWriteOptions(),
		rdOpts: db.NewReadOptions(),
	}

	return
}

func (s *RaftStorage) GetRaftSequence() RaftSequence {
	if s.lastRaftSequence != nil {
		return *(s.lastRaftSequence)
	}

	iter := s.log.CreateIterator(s.rdOpts)
	defer iter.Destroy()

	iter.SeekToLast()
	if iter.Valid() {
		key := iter.Key()
		ret, err := NewRaftSequenceFromKey(key)
		if err == nil {
			s.lastRaftSequence = ret
			return *ret
		} else {
			panic(fmt.Sprintf("parse key: %#v", err))
		}
	} else {
		return RaftSequence{}
	}
}

func (s *RaftStorage) GetCommitSequence() RaftSequence {
	if s.lastCommitSequence != nil {
		return *(s.lastCommitSequence)
	}

	iter := s.log.CreateIterator(s.rdOpts)
	defer iter.Destroy()

	iter.SeekToFirst()
	if iter.Valid() {
		key := iter.Key()
		ret, err := NewRaftSequenceFromKey(key)
		if err == nil {
			s.lastCommitSequence = ret
			return *ret
		} else {
			panic(fmt.Sprintf("parse key: %#v", err))
		}
	} else {
		// If there is no record in the log yet, fake one with
		// sequence number 0 so that it conforms with our assumption
		// that the log always keep the last record that has been
		// committed.
		errPut := s.log.Put(s.wrOpts, RaftSequence{}.AsKey(), []byte("a"))
		if errPut != nil {
			panic(fmt.Sprintf("Fails to write initial data: %#v", errPut))
		}

		return RaftSequence{}
	}
}

// Return false if the sequence number of proposed record is less
// than that of last committed record.
func (s *RaftStorage) SaveRaftRecord(seq RaftSequence, record []byte) bool {
	commitSeq := s.GetCommitSequence()
	if !commitSeq.Less(seq) {
		return false
	}

	// Adjust cached last sequence number.
	raftSeq := s.GetRaftSequence()
	if s.lastRaftSequence != nil && raftSeq.Less(seq) {
		*(s.lastRaftSequence) = seq
	}

	errPut := s.log.Put(s.wrOpts, seq.AsKey(), record)
	if errPut == nil {
		return true
	} else {
		panic(fmt.Sprintf("Fails to put: %#v", errPut))
		return false
	}
}

func (s *RaftStorage) Commit(seq RaftSequence) RaftCommitStatus {
	// Retrieve the record from log.
	logKey := seq.AsKey()
	val, getErr := s.log.Get(s.rdOpts, logKey)
	if getErr != nil {
		return COMMIT_NOT_FOUND
	}

	// Make sure that the sequence is the next one to commit.
	current := s.GetCommitSequence()
	if current.Index+1 != seq.Index {
		return COMMIT_NOT_MATCH
	}

	record, serErr := NewRaftRecord(val)
	if serErr != nil {
		return COMMIT_PARSE_ERROR
	}

	s.store.Put(record.Key, record.Value, seq.Index)

	// Adjust cached sequence number.
	if s.lastCommitSequence != nil {
		*(s.lastCommitSequence) = seq
	}

	// Adjust logs: remove all previous log records until the one
	// we just commited.
	iter := s.log.CreateIterator(s.rdOpts)
	defer iter.Destroy()
	iter.SeekToFirst()

	batch := db.NewWriteBatch()
	defer batch.Destroy()
	hasBatch := false

	for iter.Valid() {
		strKey := iter.Key()
		curKey, keyErr := NewRaftSequenceFromKey(strKey)
		if keyErr != nil {
			panic(fmt.Sprintf("malformed key: %#v", keyErr))
		}
		if !curKey.Less(seq) {
			break
		}

		if !hasBatch {
			hasBatch = true
		}

		batch.Delete(strKey)
		iter.Next()
	}

	if hasBatch {
		s.log.Write(s.wrOpts, batch)
	}

	return COMMIT_OK
}

// Help membership move or region split/merge.
func (s *RaftStorage) Close() {
	s.log.Close()
	s.store.Close()
}

// Helper method for region split.
func (s *RaftStorage) Split() (left *RaftStorage, right *RaftStorage) {
	panic("Not implemented yet")
	return
}
