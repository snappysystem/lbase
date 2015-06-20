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
package db

import (
	"math/rand"
	"path"
	"sort"
)

// A simple compactor that compacts skiplist into sstable or compact sstable from
// different levels. This implementation assumes that there is at most one
// compaction at any time.
type Compactor struct {
	manifest        *Manifest
	impl            *DbImpl
	writer          *LogWriter
	startCompaction chan chan bool
	version         int64
	maxL0Levels     int
	minTableSize    int64
	randList        []*rand.Rand
}

// Create a new compactor.
func MakeCompactor(impl *DbImpl, opt DbOption) *Compactor {
	startCompaction := make(chan chan bool, 16)
	ret := &Compactor{
		manifest:        impl.manifest,
		impl:            impl,
		writer:          impl.writer,
		startCompaction: startCompaction,
		maxL0Levels:     opt.maxL0Levels,
		minTableSize:    opt.minTableSize,
	}

	go func() {
		for finishCompaction := range ret.startCompaction {
			ret.L0Compaction()
			finishCompaction <- true
			close(finishCompaction)
		}
	}()

	return ret
}

func (c *Compactor) StartL0Compaction() chan bool {
	ret := make(chan bool)
	c.startCompaction <- ret
	return ret
}

// Perform L0 compaction: dump skiplist into a L0 table.
func (c *Compactor) L0Compaction() {
	// Get current snapshot to determine if trivial compaction or merge compaction
	// should be performed
	sId := c.manifest.GetCurrentSnapshot()
	sinfo := c.manifest.GetSnapshotInfo(sId)

	if len(sinfo) >= c.maxL0Levels && len(sinfo[c.maxL0Levels-1]) > 0 {
		c.MergeCompaction()
		return
	}

	// Let us do a trivial compaction by scanning all elements in the skiplist
	// and move them into a L0 table.
	_, oldList := c.impl.RotateSkiplist()
	iter := oldList.NewIterator(&ReadOptions{})
	iter.SeekToFirst()

	// No elements in the skiplist: nothing to do here.
	if !iter.Valid() {
		panic("Why there is no element?")
	}

	fileNumber := c.manifest.CreateFile(false)
	finfo := FileInfo{
		Location: path.Join(c.impl.GetPath(), MakeSstName(fileNumber)),
		BeginKey: iter.Key(),
		Refcnt:   1,
	}

	fh, status := c.impl.GetEnv().NewWritableFile(finfo.Location)
	if !status.Ok() {
		panic("Fails to create a new sst file!")
	}

	builder := MakeTableBuilder(fh, 2*int(c.minTableSize))

	for ; iter.Valid(); iter.Next() {
		builder.Add(iter.Key(), iter.Value())
	}

	// Get the last element
	iter.SeekToLast()
	finfo.EndKey = iter.Key()

	builder.Finalize(c.impl.GetComparator())

	newReq := NewSnapshotRequest{
		Levels: make([][]int64, 0, len(sinfo)),
		Files:  map[int64]FileInfo{fileNumber: finfo},
	}

	// Insert a new level into L0
	newReq.Levels = append(newReq.Levels, []int64{fileNumber})

	// Copy previous L0 levels over.
	idx := 0
	for ; idx < c.maxL0Levels && idx < len(sinfo) && len(sinfo[idx]) > 0; idx++ {
		tmp := make([]int64, 0, len(sinfo[idx]))
		for _, val := range sinfo[idx] {
			tmp = append(tmp, val.id)
		}
		newReq.Levels = append(newReq.Levels, tmp)
	}

	// If the corresponding L0 level already exists, skip it.
	if idx < len(sinfo) {
		idx++
	}

	// Copy all Ln levels over.
	for ; idx < len(sinfo); idx++ {
		tmp := make([]int64, 0, len(sinfo[idx]))
		for _, val := range sinfo[idx] {
			tmp = append(tmp, val.id)
		}
		newReq.Levels = append(newReq.Levels, tmp)
	}

	// Reset temp skiplist and commit table changes.
	c.impl.CompactionDone(&newReq)

	// Save the new log number, remove old log.
	oldNumber := c.manifest.LogNumber
	logNumber := ParseLogName(path.Base(c.impl.writer.file.Name()))
	c.manifest.NewLog(logNumber, false)
	oldPath := path.Join(c.impl.path, GetLogName(oldNumber))
	c.impl.env.DeleteFile(oldPath)
}

// Merge all L0 tables and some of Ln tables.
func (c *Compactor) MergeCompaction() {
	sId := c.manifest.GetCurrentSnapshot()
	sinfo := c.manifest.GetSnapshotInfo(sId)

	_, oldList := c.impl.RotateSkiplist()

	// Build heap iterator.
	skipIter := oldList.NewIterator(&ReadOptions{})
	skipIter.SeekToFirst()

	l0List := make([]Iterator, 0, c.maxL0Levels)

	if skipIter.Valid() {
		l0List = append(l0List, skipIter)
	}

	for idx, infos := range sinfo {
		if idx >= c.maxL0Levels {
			break
		}

		if len(infos) == 0 {
			continue
		}

		tid := infos[0].id
		tbl := c.impl.GetTableCache().Get(tid)
		if tbl == nil {
			panic("Expected table is not found")
		}

		iter := tbl.NewIterator()
		iter.SeekToFirst()

		l0List = append(l0List, iter)
	}

	comp := c.impl.GetComparator()
	iter := MakeHeapIterator(l0List, comp)

	// Remember the overlapping range (inclusive).
	var rangeStart, rangeEnd int

	// If we already have some Ln level tables, find overlap range.
	if len(sinfo) > c.maxL0Levels {
		concatList := make([]Iterator, 0)

		iter.SeekToFirst()
		if !iter.Valid() {
			panic("L0 tables should not be empty by now!")
		}

		beg := iter.Key()

		iter.SeekToLast()
		end := iter.Key()

		ln := sinfo[c.maxL0Levels]
		rangeStart = sort.Search(len(ln), func(i int) bool {
			return comp.Compare(ln[i].BeginKey, beg) >= 0
		})

		if rangeStart < len(ln) {
			if rangeStart > 0 &&
				comp.Compare(ln[rangeStart].BeginKey, beg) > 0 &&
				comp.Compare(ln[rangeStart-1].EndKey, beg) >= 0 {
				rangeStart--
			}
		}

		rangeEnd = sort.Search(len(ln), func(i int) bool {
			return comp.Compare(ln[i].EndKey, end) >= 0
		})

		if rangeEnd == len(ln) {
			rangeEnd--
		}

		for i := rangeStart; i <= rangeEnd; i++ {
			id := ln[i].id
			tbl := c.impl.GetTableCache().Get(id)
			concatList = append(concatList, tbl.NewIterator())
		}

		if len(concatList) > 0 {
			concatIter := &ConcatenationIterator{iters: concatList}
			concatIter.SeekToFirst()

			// Update iterator with Ln tables.
			l0List = append(l0List, iter)
			iter = MakeHeapIterator(l0List, c.impl.GetComparator())
		}
	}

	iter.SeekToFirst()
	newInfos := make([]FileInfoEx, 0)

	var previousBuilder, remainingBuilder *TableBuilder

	// Build new tables. The size of table should between minTableSize and 2*minTableSize.
	for iter.Valid() {
		fileNumber := c.manifest.CreateFile(false)
		finfo := FileInfoEx{
			FileInfo: FileInfo{
				Location: path.Join(c.impl.GetPath(), MakeSstName(fileNumber)),
				BeginKey: iter.Key(),
				Refcnt:   1,
			},
			id: fileNumber,
		}

		fh, status := c.impl.GetEnv().NewWritableFile(finfo.Location)
		if !status.Ok() {
			panic("Fails to create a new sst file!")
		}

		builder := MakeTableBuilder(fh, 2*int(c.minTableSize))
		size := int64(0)

		for size < c.minTableSize && iter.Valid() {
			key, value := iter.Key(), iter.Value()
			builder.Add(key, value)
			size = size + int64(len(key)) + int64(len(value))
			iter.Next()
		}

		aborting := false

		// If the last table is too small, it should be merged with previous
		// table. We will handle this situation out of the for loop.
		if !iter.Valid() && size < c.minTableSize && previousBuilder != nil {
			builder.Abort()
			remainingBuilder = previousBuilder
			aborting = true
		}

		// Get the last element
		if iter.Valid() {
			iter.Prev()
		} else {
			iter.SeekToLast()
		}

		finfo.EndKey = iter.Key()
		newInfos = append(newInfos, finfo)

		// Restore the original position of iterator.
		iter.Next()

		if aborting {
			break
		}

		// Delay the finalization of previous table, so that if the last table
		// is too small, use the previous table to host remaining data instead
		// of creating a new table.
		if previousBuilder != nil {
			previousBuilder.Finalize(c.impl.GetComparator())
		}

		previousBuilder = builder
	}

	// If last table is too small, do not use it. Instead, append all data to
	// previous table.
	if remainingBuilder != nil {
		lastIdx := len(newInfos) - 1
		if lastIdx < 1 {
			panic("Should have multiple tables!")
		}

		iter.Seek(newInfos[lastIdx].BeginKey)
		for ; iter.Valid(); iter.Next() {
			remainingBuilder.Add(iter.Key(), iter.Value())
		}

		iter.SeekToLast()
		newInfos[lastIdx-1].EndKey = iter.Key()
		remainingBuilder.Finalize(c.impl.GetComparator())
		newInfos = newInfos[:lastIdx]
	} else if previousBuilder != nil {
		previousBuilder.Finalize(c.impl.GetComparator())
	}

	// Prepare the new snapshot.
	newReq := NewSnapshotRequest{
		Levels: make([][]int64, 0, len(sinfo)),
		Files:  map[int64]FileInfo{},
	}

	for _, infoEx := range newInfos {
		newReq.Files[infoEx.id] = infoEx.FileInfo
	}

	if len(sinfo) > c.maxL0Levels {
		ln := sinfo[c.maxL0Levels]
		left := ln[:rangeStart]
		right := ln[rangeEnd+1:]

		newLevel := make([]FileInfoEx, 0)

		for _, val := range left {
			newLevel = append(newLevel, val)
		}

		for _, val := range newInfos {
			newLevel = append(newLevel, val)
		}

		for _, val := range right {
			newLevel = append(newLevel, val)
		}

		sinfo[c.maxL0Levels] = newLevel
	} else if len(sinfo) == c.maxL0Levels {
		sinfo = append(sinfo, newInfos)
	} else {
		panic("Unexpected sinfo length!")
	}

	// Remove all L0 files.
	// (TODO) decrease refcont?
	for i := 0; i < c.maxL0Levels; i++ {
		sinfo[i] = sinfo[i][:0]
	}

	for _, l := range sinfo {
		intList := make([]int64, 0)
		for _, val := range l {
			intList = append(intList, val.id)
		}

		newReq.Levels = append(newReq.Levels, intList)
	}

	// Reset temp skiplist and commit table changes.
	c.impl.CompactionDone(&newReq)
}

// Merge tables in non-L0 levels.
func (c *Compactor) LnCompaction(level int) {
	if level < c.maxL0Levels {
		panic("Only compact non-L0 levels")
	}

	// Get current snapshot to determine if trivial compaction or merge compaction
	// should be performed
	sId := c.manifest.GetCurrentSnapshot()
	sinfo := c.manifest.GetSnapshotInfo(sId)

	if len(sinfo) < level {
		panic("manifest does not have required level!")
	}

	for len(c.randList) <= level {
		c.randList = append(c.randList, rand.New(rand.NewSource(0)))
	}

	// Pick a non-negative index value on level @level to be compacted.
	idx := c.randList[level].Intn(len(sinfo[level]))

	// Build heap iterator.
	iterList := make([]Iterator, 0)

	tid := sinfo[level][idx].id
	tbl := c.impl.GetTableCache().Get(tid)

	if tbl == nil {
		panic("Expected table is not found")
	}

	iter := tbl.NewIterator()
	iter.SeekToFirst()

	iterList = append(iterList, iter)
	comp := c.impl.GetComparator()

	// Remember the overlapping range (inclusive).
	var rangeStart, rangeEnd int

	// If we already have some Ln level tables, find overlap range.
	if len(sinfo) > level {
		concatList := make([]Iterator, 0)

		iter.SeekToFirst()
		if !iter.Valid() {
			panic("L0 tables should not be empty by now!")
		}

		beg := iter.Key()

		iter.SeekToLast()
		end := iter.Key()

		ln := sinfo[c.maxL0Levels]
		rangeStart = sort.Search(len(ln), func(i int) bool {
			return comp.Compare(ln[i].BeginKey, beg) >= 0
		})

		if rangeStart < len(ln) {
			if rangeStart > 0 &&
				comp.Compare(ln[rangeStart].BeginKey, beg) > 0 &&
				comp.Compare(ln[rangeStart-1].EndKey, beg) >= 0 {
				rangeStart--
			}
		}

		rangeEnd = sort.Search(len(ln), func(i int) bool {
			return comp.Compare(ln[i].EndKey, end) >= 0
		})

		if rangeEnd == len(ln) {
			rangeEnd--
		}

		for i := rangeStart; i <= rangeEnd; i++ {
			id := ln[i].id
			tbl := c.impl.GetTableCache().Get(id)
			concatList = append(concatList, tbl.NewIterator())
		}

		concatIter := &ConcatenationIterator{iters: concatList}
		concatIter.SeekToFirst()

		// Update iterator with Ln tables.
		iterList = append(iterList, concatIter)
		iter = MakeHeapIterator(iterList, comp)
	}

	iter.SeekToFirst()
	newInfos := make([]FileInfoEx, 0)

	var previousBuilder, remainingBuilder *TableBuilder

	// Build new tables. The size of table should between minTableSize and 2*minTableSize.
	for iter.Valid() {
		fileNumber := c.manifest.CreateFile(false)
		finfo := FileInfoEx{
			FileInfo: FileInfo{
				Location: path.Join(c.impl.GetPath(), MakeSstName(fileNumber)),
				BeginKey: iter.Key(),
				Refcnt:   1,
			},
			id: fileNumber,
		}

		fh, status := c.impl.GetEnv().NewWritableFile(finfo.Location)
		if !status.Ok() {
			panic("Fails to create a new sst file!")
		}

		builder := MakeTableBuilder(fh, 2*int(c.minTableSize))
		size := int64(0)

		for ; iter.Valid() && size < c.minTableSize; iter.Next() {
			builder.Add(iter.Key(), iter.Value())
		}

		newInfos = append(newInfos, finfo)

		// If the last table is too small, it should be merged with previous
		// table. We will handle this situation out of the for loop.
		if !iter.Valid() && size < c.minTableSize {
			builder.Abort()
			remainingBuilder = previousBuilder
			break
		}

		// Get the last element
		iter.Prev()
		finfo.EndKey = iter.Key()
		iter.Next()

		// Delay the finalization of previous table, so that if the last table
		// is too small, use the previous table to host remaining data instead
		// of creating a new table.
		if previousBuilder != nil {
			previousBuilder.Finalize(c.impl.GetComparator())
		}

		previousBuilder = builder
	}

	// If last table is too small, do not use it. Instead, append all data to
	// previous table.
	if remainingBuilder != nil {
		lastIdx := len(newInfos) - 1
		if lastIdx < 1 {
			panic("Should have multiple tables!")
		}

		iter.Seek(newInfos[lastIdx].BeginKey)
		for ; iter.Valid(); iter.Next() {
			remainingBuilder.Add(iter.Key(), iter.Value())
		}

		iter.Prev()
		newInfos[lastIdx-1].EndKey = iter.Key()
		remainingBuilder.Finalize(c.impl.GetComparator())
		newInfos = newInfos[:lastIdx]
	} else if previousBuilder != nil {
		previousBuilder.Finalize(c.impl.GetComparator())
	}

	// Prepare the new snapshot.
	newReq := NewSnapshotRequest{
		Levels: make([][]int64, 0, len(sinfo)),
		Files:  map[int64]FileInfo{},
	}

	for _, infoEx := range newInfos {
		newReq.Files[infoEx.id] = infoEx.FileInfo
	}

	if len(sinfo) > level {
		ln := sinfo[level+1]
		left := ln[:rangeStart]
		right := ln[rangeEnd+1:]

		newLevel := make([]FileInfoEx, 0)

		for _, val := range left {
			newLevel = append(newLevel, val)
		}

		for _, val := range newInfos {
			newLevel = append(newLevel, val)
		}

		for _, val := range right {
			newLevel = append(newLevel, val)
		}

		sinfo[c.maxL0Levels] = newLevel
	} else if len(sinfo) == level {
		sinfo = append(sinfo, newInfos)
	} else {
		panic("Unexpected sinfo length!")
	}

	// Remove old table.
	linfo := sinfo[level][:idx]
	sinfo[level] = append(linfo[:idx], linfo[idx+1:]...)

	for _, l := range sinfo {
		intList := make([]int64, 0)
		for _, val := range l {
			intList = append(intList, val.id)
		}

		newReq.Levels = append(newReq.Levels, intList)
	}

	c.manifest.NewSnapshot(&newReq, false)
}
