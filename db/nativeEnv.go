package db

import (
	"io"
	"os"
)

// A native file api wrapper.
type localSequentialFile struct {
	file *os.File
	pos  int64
}

func MakeLocalSequentialFile(name string) SequentialFile {
	f, err := os.Open(name)
	if err != nil {
		return nil
	}

	return &localSequentialFile{f, 0}
}

func (a *localSequentialFile) Read(scratch []byte) (ret []byte, s Status) {
	nreads, err := a.file.ReadAt(scratch, a.pos)
	switch {
	case err == nil:
		s = MakeStatusOk()
		ret = scratch[:nreads]
		a.pos = a.pos + int64(nreads)
	case err == io.EOF:
		s = MakeStatusOk()
	default:
		s = MakeStatusIoError("fails to read")
	}
	return
}

func (a *localSequentialFile) Skip(n int64) Status {
	a.pos = a.pos + n
	return MakeStatusOk()
}

func (a *localSequentialFile) Close() {
	a.file.Close()
}

// A native file api wrapper.
type localWritableFile struct {
	name string
	file *os.File
	pos  int64
}

func MakeLocalWritableFile(name string) WritableFile {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil
	}

	off, err2 := f.Seek(0, 2)
	if err2 != nil {
		return nil
	}

	return &localWritableFile{name, f, off}
}

func (a *localWritableFile) Append(data []byte) Status {
	nwritten, err := a.file.WriteAt(data, a.pos)
	if err != nil || int(nwritten) != len(data) {
		return MakeStatusIoError("")
	} else {
		a.pos = a.pos + int64(nwritten)
		return MakeStatusOk()
	}
}

func (a *localWritableFile) Size() int64 {
	return a.pos
}

func (a *localWritableFile) Close() Status {
	err := a.file.Close()
	if err != nil {
		return MakeStatusIoError("")
	} else {
		return MakeStatusOk()
	}
}

func (a *localWritableFile) Flush() Status {
	err := a.file.Sync()
	if err != nil {
		return MakeStatusIoError("")
	} else {
		return MakeStatusOk()
	}
}

func (a *localWritableFile) Name() string {
	return a.name
}

// A native file api wrapper.
type localRandomAccessFile struct {
	file *os.File
}

func MakeLocalRandomAccessFile(name string) RandomAccessFile {
	f, err := os.Open(name)
	if err != nil {
		return nil
	}
	return &localRandomAccessFile{f}
}

func (a *localRandomAccessFile) Read(off int64, scratch []byte) (b []byte, s Status) {
	nreads, err := a.file.ReadAt(scratch, off)
	if err != nil {
		s = MakeStatusIoError("")
		return
	}
	b = scratch[:nreads]
	s = MakeStatusOk()
	return
}

func (a *localRandomAccessFile) Close() {
	a.file.Close()
}

// Default native Env class.
type NativeEnv int

func MakeNativeEnv() Env {
	return NativeEnv(0)
}

func (a NativeEnv) NewSequentialFile(name string) (f SequentialFile, s Status) {
	f = MakeLocalSequentialFile(name)
	if f == nil {
		s = MakeStatusIoError("")
	} else {
		s = MakeStatusOk()
	}
	return
}

func (a NativeEnv) NewRandomAccessFile(n string) (f RandomAccessFile, s Status) {
	f = MakeLocalRandomAccessFile(n)
	if f == nil {
		s = MakeStatusIoError("")
	} else {
		s = MakeStatusOk()
	}
	return
}

func (a NativeEnv) NewWritableFile(name string) (f WritableFile, s Status) {
	f = MakeLocalWritableFile(name)
	if f == nil {
		s = MakeStatusIoError("")
	} else {
		s = MakeStatusOk()
	}
	return
}

func (a NativeEnv) FileExists(name string) bool {
	f, err := os.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	} else {
		f.Close()
	}
	return true
}

func (a NativeEnv) GetChildren(dir string) (list []string, s Status) {
	f, err := os.Open(dir)
	if err != nil {
		s = MakeStatusIoError("")
		return
	}

	list, err = f.Readdirnames(0)
	if err != nil {
		s = MakeStatusIoError("")
	} else {
		s = MakeStatusOk()
	}
	return
}

func (a NativeEnv) DeleteFile(name string) Status {
	err := os.Remove(name)
	if err != nil {
		return MakeStatusIoError("")
	} else {
		return MakeStatusOk()
	}
}

func (a NativeEnv) CreateDir(dir string) Status {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return MakeStatusIoError("")
	} else {
		return MakeStatusOk()
	}
}

func (a NativeEnv) DeleteDir(dir string) Status {
	err := os.RemoveAll(dir)
	if err != nil {
		return MakeStatusIoError("")
	} else {
		return MakeStatusOk()
	}
}

func (a NativeEnv) GetFileSize(name string) (size uint64, s Status) {
	fi, err := os.Stat(name)
	if err != nil {
		s = MakeStatusIoError("")
	} else {
		s = MakeStatusOk()
		size = uint64(fi.Size())
	}
	return
}

func (a NativeEnv) RenameFile(src string, target string) Status {
	err := os.Rename(src, target)
	if err != nil {
		return MakeStatusIoError("")
	} else {
		return MakeStatusOk()
	}
}
