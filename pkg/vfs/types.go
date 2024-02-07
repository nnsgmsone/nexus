package vfs

import (
	"os"
)

type File interface {
	Fd() uintptr
	Sync() error
	Close() error
	Stat() (os.FileInfo, error)
	Read(p []byte) (n int, err error)
	ReadAt(p []byte, off int64) (n int, err error)
	Write(p []byte) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
}

type FS interface {
	ChDir(dir string) error
	Mkdir(name string) error
	// Remove removes the named file or directory and any children
	// it contains. It removes evertyhing it can but returns the
	// first error it encounters. If the path does not exist, Remove
	// returns nil (no error).
	Remove(name string) error
	// Open opens the named file for reading and writing, If the
	// file does not exist, it creates an empty file.
	Open(name string) (File, error)
	Stat(name string) (os.FileInfo, error)
	ReadDir(name string) ([]os.DirEntry, error)

	ReadFile(name string) ([]byte, error)
	WriteFile(name string, data []byte) error
}
