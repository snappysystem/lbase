package server

import (
	"bytes"
	"crypto/sha1"
	"lbase/balancer"
	"log"
	"net/rpc"
	"time"
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
	Region balancer.Region
	MaxRecords int
	RPCTimeoutMs time.Duration
}

// Given the starting pending queue sequence numbers, collect all new records
// since those starting sequences.
func (c *RecordCollector) Collect(
	raft *RaftStates,
	ss map[balancer.ServerName]int64) CollectResults {
	// A structure to hold client and results.
	type ClientCxt struct {
		cli *rpc.Client
		call *rpc.Call
	}

	var res CollectResults
	waitChan := time.After(c.RPCTimeoutMs * time.Millisecond)

	// Send collect request to quorum members.
	cliMap := make(map[balancer.ServerName]ClientCxt)
	callName := "ServerRPC.GetNRecords"
	for sn,startSeq := range ss {
		cli := raft.GetClient(sn)
		if cli == nil {
			continue
		}
		req := GetNRecordsRequest{
			Region: c.Region,
			StartSequence: startSeq,
			NumberOfRecords: c.MaxRecords,
		}
		resp := GetNRecordsReply{}
		cliMap[sn] = ClientCxt{
			cli: cli,
			call: cli.Go(callName, &req, &resp, nil),
		}
	}

	// If we cannot contact with majority of the quorum, there is little
	// chance that some of the data we get should be saved. So simply
	// tell the caller that this is not a good time to collect record.
	if len(cliMap) < len(ss) / 2 + 1 {
		return res
	}

	// Collect replies from RPC.
	respMap := make(map[balancer.ServerName][][]byte)
	for sn,cxt := range cliMap {
		select {
		case <-cxt.call.Done:
			resp := cxt.call.Reply.(GetNRecordsReply)
			if resp.Ok {
				respMap[sn] = resp.Records
			}
		case <-waitChan:
			waitChan = time.After(time.Duration(0))
		}
	}

	// First pass to identify consensus records.
	type ValueInfo struct {
		Val []byte
		NameList []balancer.ServerName
		IdxList []int
	}

	dedupMap := make(map[string]ValueInfo)
	progMap := make(map[balancer.ServerName]int)

	for sn,list := range respMap {
		progMap[sn] = 0
		for idx,val := range list {
			bk := sha1.Sum(val)
			key := string(bk[:20])
			vi, found := dedupMap[key]
			if found && bytes.Compare(vi.Val, val) != 0 {
				log.Panic("Fails to match deduped value")
			}
			if !found {
				vi.Val = val
			}
			vi.NameList = append(vi.NameList, sn)
			vi.IdxList = append(vi.IdxList, idx)
			dedupMap[key] = vi
		}
	}

	// If a server is unavailable, assume that it has all records.
	//base := len(ss) - len(respMap)
	//majority := len(ss) / 2 + 1 - base

	// Second pass to figure out the order of records.

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
