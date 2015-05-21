package server

import (
	"lbase/db"
)

type RaftStorage struct {
	db   *db.Db
	opts *RaftOptions
}

func NewRaftStorage(opts *RaftOptions) *RaftStorage {
	return nil
}

func (s *RaftStorage) GetRaftSequence() RaftSequence {
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
