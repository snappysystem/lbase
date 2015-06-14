package server

import (
	"lbase/balancer"
)

type CollectResults struct {
	// Pending queue sequence numbers.
	EndSequences map[balancer.ServerName]int64
	// Consensus mutations to be saved.
	Mutations [][]byte
	// Hashes of consensus mutations to be saved.
	Hashes []int64
}

type RecordCollector struct {
}

// Given the starting pending queue sequence numbers, collect all new records
// since those starting sequences.
func (col *RecordCollector) Collect(ss map[balancer.ServerName]int) CollectResults {
	return CollectResults{}
}

// Advise individual servers to trim pending queue up to the sequence numbers
// in the map @ss.
func (col *RecordCollector) Trim(ss map[balancer.ServerName]int64) {
}

// Compute the hash of a value.
func (col *RecordCollector) Hash(val []byte) int64 {
	return int64(0)
}
