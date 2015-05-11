package db

// Implement a record based log stream.
//
// Action (or redo) logs are used widely in DB implementations to help
// recovery from crash. Each action is stored as a record in the log
// file
//
// A server may crash at any time. The last record in the stream may be
// partial and should be identified and discarded during recovery later;
//
// The implementation uses crc checksum to identify corruptted record
// at the end of log stream.

import (
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"unsafe"
)

const (
	// size of a log block
	kBlockSize = 32768
	// header includes checksum (4 bytes), type (1 byte), length (2 bytes)
	kHeaderSize = 4 + 1 + 2

	// A single, full record
	kFullType = 1

	// Following are for fragments
	kFirstType  = 2
	kMiddleType = 3
	kLastType   = 4

	// Base name suffix for a log file.
	kLogSuffix = "_db.log"
)

type LogWriter struct {
	file WritableFile
}

// Given full path, return a new log writer
func MakeLogWriter(e Env, fpath string) *LogWriter {
	wf, status := e.NewWritableFile(fpath)
	if !status.Ok() {
		return nil
	}

	return &LogWriter{file: wf}
}

func (w *LogWriter) Name() string {
	return w.file.Name()
}

func (w *LogWriter) AddRecord(record []byte) Status {
	header := [kHeaderSize]byte{}

	for firstIter := true; true; firstIter = false {
		off := w.file.Size()
		offInBlock := int(off % kBlockSize)
		availInBlock := kBlockSize - offInBlock
		totalBytes := kHeaderSize + len(record)

		switch {
		case totalBytes <= availInBlock:
			p32 := (*uint32)(unsafe.Pointer(&header[0]))
			*p32 = crc32.ChecksumIEEE(record)

			if firstIter {
				// In most case, entire record fit into a block
				header[4] = kFullType
			} else {
				// sometimes, the last piece of a record is in a new block
				header[4] = kLastType
			}

			p16 := (*uint16)(unsafe.Pointer(&header[5]))
			*p16 = uint16(totalBytes)

			s := w.file.Append(header[:])
			if !s.Ok() {
				return s
			}

			return w.file.Append(record)

		case availInBlock > kHeaderSize:
			fragment := availInBlock - kHeaderSize

			p32 := (*uint32)(unsafe.Pointer(&header[0]))
			*p32 = crc32.ChecksumIEEE(record[:fragment])

			if firstIter {
				header[4] = kFirstType
			} else {
				header[4] = kMiddleType
			}

			p16 := (*uint16)(unsafe.Pointer(&header[5]))
			*p16 = uint16(availInBlock)

			s := w.file.Append(header[:])
			if !s.Ok() {
				return s
			}

			s = w.file.Append(record[:fragment])
			if !s.Ok() {
				return s
			}

			record = record[fragment:]

		case firstIter:
			// if there is too little space in current block,
			// skip the remaining bytes and start in a new block
			s := w.file.Append(header[:availInBlock])
			if !s.Ok() {
				return s
			}

		default:
			panic("too little space left should only occur at first iter")
		}
	}

	panic("Should not reach here")
	return MakeStatusOk()
}

func (w *LogWriter) Close() {
	w.file.Close()
}

const (
	ReadStatusOk = iota
	ReadStatusEOF
	ReadStatusCorruption
)

type LogReader struct {
	file     SequentialFile
	off      int64
	checksum bool
}

// Given a full path, return a reader object
func MakeLogReader(e Env, fpath string, off int64, checksum bool) *LogReader {
	rf, status := e.NewSequentialFile(fpath)
	if !status.Ok() {
		return nil
	}

	status = rf.Skip(off)
	if !status.Ok() {
		return nil
	}

	return &LogReader{
		file:     rf,
		off:      off,
		checksum: checksum,
	}
}

func (r *LogReader) ReadRecord(scratch []byte) (ret []byte, status int) {
	header := [kHeaderSize]byte{}
	buffer := scratch

	// size of the record
	size := 0
	// current offset in the file
	off := r.off

	for firstIter := true; true; firstIter = false {
		offInBlock := off % kBlockSize
		availInBlock := int(kBlockSize - offInBlock)

		switch {
		case availInBlock > kHeaderSize:
			tmp, s := r.file.Read(header[:])
			off = off + int64(len(tmp))

			switch {
			case !s.Ok():
				status = ReadStatusCorruption
				return
			case len(tmp) == kHeaderSize:
				// expected case, do nothing
			case len(tmp) == 0:
				status = ReadStatusEOF
				return
			default:
				status = ReadStatusCorruption
				return
			}

			p16 := (*uint16)(unsafe.Pointer(&header[5]))
			totalBytes := int(*p16)

			if totalBytes <= kHeaderSize || totalBytes > availInBlock {
				status = ReadStatusCorruption
				return
			}

			toRead := totalBytes - kHeaderSize
			size = size + toRead

			tmp, s = r.file.Read(buffer[:toRead])
			off = off + int64(len(tmp))

			if !s.Ok() || len(tmp) != toRead {
				status = ReadStatusCorruption
				return
			}

			p32 := (*uint32)(unsafe.Pointer(&header[5]))
			cksum := crc32.ChecksumIEEE(tmp)

			if cksum != *p32 {
				status = ReadStatusCorruption
			}

			switch int(header[4]) {
			case kFullType:
				if firstIter {
					ret, status = tmp, ReadStatusOk
					r.off = off
				} else {
					status = ReadStatusCorruption
				}
				return

			case kLastType:
				if firstIter {
					status = ReadStatusCorruption
				} else {
					ret, status = scratch[:size], ReadStatusOk
					r.off = off
				}
				return

			default:
				// continue reading
				buffer = buffer[toRead:]
			}

		default:
			s := r.file.Skip(int64(availInBlock))
			off = off + int64(availInBlock)

			if !s.Ok() {
				status = ReadStatusCorruption
				return
			}
		}
	}

	panic("Should not reach here!")
	return
}

// Given a log file's base name, return the log number. If the base name
// is not valid, return a negative value
func ParseLogName(basename string) int64 {
	if !strings.HasSuffix(basename, kLogSuffix) {
		return -1
	}

	numStr := strings.TrimRight(basename, kLogSuffix)
	if len(numStr) == 0 {
		return -1
	}

	ret, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return -1
	}

	return ret
}

// Given a log number, return a wellformed log file base name.
func GetLogName(logNumber int64) string {
	return fmt.Sprintf("%010d%s", logNumber, kLogSuffix)
}
