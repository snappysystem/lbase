package db

import (
	"path"
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

// Perform L0 compaction: dump skiplist into a table.
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

	builder := MakeTableBuilder(fh)

	for iter.Valid() {
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
}

func (c *Compactor) LnCompaction() {
}
