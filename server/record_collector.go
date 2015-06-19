package server

import (
	"bytes"
	"crypto/sha1"
	"lbase/balancer"
	"log"
	"time"
)

type CollectResults struct {
	// Pending queue sequence numbers.
	EndSequences map[balancer.ServerName]int64
	// Consensus mutations to be saved.
	Mutations [][]byte
	// Hashes of consensus mutations to be saved.
	Hashes []string
}

type RecordCollector struct {
	Region       balancer.Region
	MaxRecords   int
	RPCTimeoutMs time.Duration
}

// Given the starting pending queue sequence numbers, collect all new records
// since those starting sequences.
// For simplicity, we do not keep the order when a pending request is submitted.
// A request submitted later may appear early in the output of Collect() method.
// This is justified because under certain circumstances (for example, members
// of the quorum is not available), we simply do not have enough information
// to figure out the order.
func (c *RecordCollector) Collect(
	raft *RaftStates,
	ss map[balancer.ServerName]int64) CollectResults {

	var res CollectResults
	waitChan := time.After(c.RPCTimeoutMs * time.Millisecond)

	// Send collect request to quorum members.
	cliMap := make(map[balancer.ServerName]RequestInfo)
	callName := "ServerRPC.GetNRecords"
	for sn, startSeq := range ss {
		cli := raft.GetClient(sn)
		if cli == nil {
			continue
		}
		req := GetNRecordsRequest{
			Region:          c.Region,
			StartSequence:   startSeq,
			NumberOfRecords: c.MaxRecords,
		}
		resp := GetNRecordsReply{}
		cliMap[sn] = RequestInfo{
			Cli:  cli,
			Call: cli.Go(callName, &req, &resp, nil),
		}
	}

	// If we cannot contact with majority of the quorum, there is little
	// chance that some of the data we get should be saved. So simply
	// tell the caller that this is not a good time to collect record.
	if len(cliMap) < len(ss)/2+1 {
		return res
	}

	// Collect replies from RPC.
	respMap := make(map[balancer.ServerName][][]byte)
	for sn, cxt := range cliMap {
		select {
		case <-cxt.Call.Done:
			resp := cxt.Call.Reply.(GetNRecordsReply)
			if resp.Ok {
				respMap[sn] = resp.Records
			}
			raft.ReturnClient(sn, cxt.Cli)
		case <-waitChan:
			waitChan = time.After(time.Duration(0))
		}
	}

	// First pass to identify consensus records.
	type ValueInfo struct {
		Val   []byte
		Count int
	}

	dedupMap := make(map[string]ValueInfo)
	for _, list := range respMap {
		for _, val := range list {
			bk := sha1.Sum(val)
			key := string(bk[:20])
			vi, found := dedupMap[key]
			if found && bytes.Compare(vi.Val, val) != 0 {
				log.Panic("Fails to match deduped value")
			}
			if !found {
				vi.Val = val
			}
			vi.Count++
			dedupMap[key] = vi
		}
	}

	// If a server is unavailable, assume that it has all records.
	base := len(ss) - len(respMap)
	threshold := len(ss)/2 - base

	// Second pass to figure out the order of records.
	for key, vi := range dedupMap {
		if vi.Count > threshold {
			res.Mutations = append(res.Mutations, vi.Val)
			res.Hashes = append(res.Hashes, key)
		}
	}

	// Third pass to figure out new last sequences.
	res.EndSequences = make(map[balancer.ServerName]int64)
	for sn, list := range respMap {
		idx := len(list) - 1
		for idx >= 0 {
			bk := sha1.Sum(list[idx])
			key := string(bk[:20])
			vi, _ := dedupMap[key]
			if vi.Count > threshold {
				base, _ := ss[sn]
				res.EndSequences[sn] = base + int64(idx)
				break
			}
			idx++
		}
	}

	return CollectResults{}
}

// Advise individual servers to trim pending queue up to the sequence numbers
// in the map @ss.
func (c *RecordCollector) Trim(raft *RaftStates, ss map[balancer.ServerName]int64) {
	// Send collect request to quorum members.
	cliMap := make(map[balancer.ServerName]RequestInfo)
	callName := "ServerRPC.TrimPendingQueue"
	waitChan := time.After(c.RPCTimeoutMs * time.Millisecond)

	for sn, seq := range ss {
		cli := raft.GetClient(sn)
		if cli == nil {
			continue
		}
		req := TrimPendingQueueRequest{
			Region:      c.Region,
			EndSequence: seq,
		}
		resp := TrimPendingQueueReply{}
		cliMap[sn] = RequestInfo{
			Cli:  cli,
			Call: cli.Go(callName, &req, &resp, nil),
		}
	}

	for sn, ri := range cliMap {
		select {
		case <-ri.Call.Done:
			raft.ReturnClient(sn, ri.Cli)
		case <-waitChan:
			waitChan = time.After(time.Duration(0))
		}
	}
}

// Compute the hash of a value.
func (col *RecordCollector) Hash(val []byte) string {
	bk := sha1.Sum(val)
	return string(bk[:20])
}
