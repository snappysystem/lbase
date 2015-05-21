package server

import (
	"lbase/db"
)

type RaftStorage struct {
	// Raft logs are kept in a separate db.
	log db.Db
	// Stores the committed data.
	rdb *RegionDb
	// Various options for raft storage.
	opts *RaftOptions
	// Latest sequence number in the log.
	lastRaftSequence *RaftSequence
	// Last committed sequence number.
	lastDbSequence int64
}

func NewRaftStorage(opts *RaftOptions, rdb *RegionDb) (ret *RaftStorage, err error) {
	// Create a log db if we have not done so yet.
	logopts := db.NewDbOptions()
	logopts.SetCreateIfMissing(1)

	log, openError := db.OpenDb(logopts, opts.LogDbRoot)
	if openError != nil {
		err = openError
		return
	}

	ret = &RaftStorage{
		log:  log,
		rdb:  rdb,
		opts: opts,
	}

	return
}

func (s *RaftStorage) GetRaftSequence() RaftSequence {
	if s.lastRaftSequence != nil {
		return *(s.lastRaftSequence)
	}

	iter := s.log.CreateIterator(db.NewReadOptions())
	defer iter.Destroy()

	iter.SeekToLast()
	//if iter.Valid() {
	return RaftSequence{}
}

func (s *RaftStorage) CheckAndSetRaftSequence(seq RaftSequence) bool {
	return false
}

func (s *RaftStorage) SaveRecord(record []byte) bool {
	return false
}

func (s *RaftStorage) Commit(seq RaftSequence) bool {
	return false
}
