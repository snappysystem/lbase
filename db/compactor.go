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
	manifest         *Manifest
	impl             *DbImpl
	writer           *LogWriter
	startCompaction  chan bool
	finishCompaction chan bool
	version          int64
	minLogSize       int64
	maxL0Levels      int
	minTableSize     int64
	randList         []*rand.Rand
}

// This should be run continuously from a go routine to find files that need to be compacted
func (c *Compactor) Check() {
	// First check if a L0 compaction is warrented.
	logSize := c.writer.file.Size()
	if logSize > c.minLogSize {
		go c.L0Compaction()
		return
	}
}

// Perform L0 compaction: dump skiplist into a L0 table.
func (c *Compactor) L0Compaction() {
	// Get current snapshot to determine if trivial compaction or merge compaction
	// should be performed
	sId := c.manifest.GetCurrentSnapshot()
	sinfo := c.manifest.GetSnapshotInfo(sId)

	if len(sinfo) > c.maxL0Levels {
		if len(sinfo[c.maxL0Levels-1]) > 0 {
			panic("L0 level has already full!")
		}
		if c.maxL0Levels > 1 && len(sinfo[c.maxL0Levels-2]) > 0 {
			c.MergeCompaction()
			return
		}
	}

	// Let us do a trivial compaction by scanning all elements in the skiplist
	// and move them into a L0 table.
	_, oldList := c.impl.RotateSkiplist()
	iter := oldList.NewIterator(&ReadOptions{})
	iter.SeekToFirst()

	// No elements in the skiplist: nothing to do here.
	if !iter.Valid() {
		return
	}

	fileNumber := c.manifest.CreateFile(false)
	finfo := FileInfo{
		Location: path.Join(c.impl.GetPath(), MakeManifestName(fileNumber)),
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
	iter.Prev()
	finfo.EndKey = iter.Key()

	builder.Finalize(c.impl.GetComparator())

	newReq := NewSnapshotRequest{
		Levels: make([][]int64, 0, len(sinfo)),
		Files:  map[int64]FileInfo{fileNumber: finfo},
	}

	// Copy previous L0 levels over.
	idx := 0
	for ; idx < c.maxL0Levels && idx < len(sinfo) && len(sinfo[idx]) > 0; idx++ {
		tmp := make([]int64, 0, len(sinfo[idx]))
		for _, val := range sinfo[idx] {
			tmp = append(tmp, val.id)
		}
		newReq.Levels = append(newReq.Levels, tmp)
	}

	// Insert a new level into L0
	newReq.Levels = append(newReq.Levels, []int64{fileNumber})

	// Copy all Ln levels over.
	for ; idx < len(sinfo); idx++ {
		tmp := make([]int64, 0, len(sinfo[idx]))
		for _, val := range sinfo[idx] {
			tmp = append(tmp, val.id)
		}
		newReq.Levels = append(newReq.Levels, tmp)
	}

	c.manifest.NewSnapshot(&newReq, false)
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

		concatIter := &ConcatenationIterator{iters: concatList}
		concatIter.SeekToFirst()

		// Update iterator with Ln tables.
		l0List = append(l0List, iter)
		iter = MakeHeapIterator(l0List, c.impl.GetComparator())
	}

	iter.SeekToFirst()
	newInfos := make([]FileInfoEx, 0)

	var previousBuilder, remainingBuilder *TableBuilder

	// Build new tables. The size of table should between minTableSize and 2*minTableSize.
	for iter.Valid() {
		fileNumber := c.manifest.CreateFile(false)
		finfo := FileInfoEx{
			FileInfo: FileInfo{
				Location: path.Join(c.impl.GetPath(), MakeManifestName(fileNumber)),
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

	for _, l := range sinfo {
		intList := make([]int64, 0)
		for _, val := range l {
			intList = append(intList, val.id)
		}

		newReq.Levels = append(newReq.Levels, intList)
	}

	c.manifest.NewSnapshot(&newReq, false)
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
				Location: path.Join(c.impl.GetPath(), MakeManifestName(fileNumber)),
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
