package vfs

import (
	"io"
	"os"
	"sync"
	"time"
)

// only use for test
type memFS struct {
	sync.Mutex
	entries map[string]*memFile
}

type memFile struct {
	sync.Mutex
	name string
	data []byte
}

type memFileFd struct {
	off  int64
	file *memFile
}

func NewMemFS() FS {
	return &memFS{
		entries: make(map[string]*memFile),
	}
}

func (fs *memFS) ChDir(dir string) error {
	return nil
}

func (fs *memFS) Mkdir(name string) error {
	return nil
}

func (fs *memFS) Remove(name string) error {
	fs.Lock()
	defer fs.Unlock()
	delete(fs.entries, name)
	return nil
}

func (fs *memFS) Open(name string) (File, error) {
	fs.Lock()
	defer fs.Unlock()
	if file, ok := fs.entries[name]; ok {
		return &memFileFd{file: file}, nil
	}
	fs.entries[name] = &memFile{name: name}
	return &memFileFd{file: fs.entries[name]}, nil
}

func (fs *memFS) Stat(name string) (os.FileInfo, error) {
	fs.Lock()
	defer fs.Unlock()
	if file, ok := fs.entries[name]; ok {
		return &memFileFd{file: file}, nil
	}
	return nil, os.ErrNotExist
}

func (fs *memFS) ReadDir(name string) ([]os.DirEntry, error) {
	return nil, nil
}

func (fs *memFS) ReadFile(name string) ([]byte, error) {
	fs.Lock()
	defer fs.Unlock()
	if file, ok := fs.entries[name]; ok {
		return file.data, nil
	}
	return nil, os.ErrNotExist
}

func (fs *memFS) WriteFile(name string, data []byte) error {
	fs.Lock()
	defer fs.Unlock()
	fs.entries[name] = &memFile{
		name: name,
		data: data,
	}
	return nil
}

func (fp *memFileFd) Fd() uintptr {
	return 0
}

func (fp *memFileFd) Sync() error {
	return nil
}

func (fp *memFileFd) Close() error {
	return nil
}

func (fp *memFileFd) Stat() (os.FileInfo, error) {
	return fp, nil
}

func (fp *memFileFd) Read(p []byte) (int, error) {
	if fp.off >= int64(len(fp.file.data)) {
		return 0, io.EOF
	}
	fp.file.Lock()
	defer fp.file.Unlock()
	return copy(p, fp.file.data[fp.off:]), nil
}

func (fp *memFileFd) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(fp.file.data)) {
		return 0, io.EOF
	}
	fp.file.Lock()
	defer fp.file.Unlock()
	return copy(p, fp.file.data[off:]), nil
}

func (fp *memFileFd) Write(p []byte) (int, error) {
	fp.file.Lock()
	defer fp.file.Unlock()
	fp.file.data = append(fp.file.data[fp.off:], p...)
	return len(p), nil
}

func (fp *memFileFd) WriteAt(p []byte, off int64) (int, error) {
	fp.file.Lock()
	defer fp.file.Unlock()
	fp.file.data = append(fp.file.data[off:], p...)
	return len(p), nil
}

func (fp *memFileFd) Name() string {
	return fp.file.name
}

func (fp *memFileFd) Size() int64 {
	return int64(len(fp.file.data))
}

func (fp *memFileFd) Mode() os.FileMode {
	return 0
}

func (fp *memFileFd) ModTime() time.Time {
	return time.Time{}
}

func (fp *memFileFd) IsDir() bool {
	return false
}

func (fp *memFileFd) Sys() any {
	return nil
}
