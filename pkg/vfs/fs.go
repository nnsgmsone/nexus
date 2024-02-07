package vfs

import "os"

type DefaultFS struct {
}

func NewFS() *DefaultFS {
	return &DefaultFS{}
}

func (fs *DefaultFS) ChDir(dir string) error {
	return os.Chdir(dir)
}

func (fs *DefaultFS) Mkdir(name string) error {
	return os.Mkdir(name, 0664)
}

func (fs *DefaultFS) Remove(name string) error {
	return os.RemoveAll(name)
}

func (fs *DefaultFS) Open(name string) (File, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0664)
}

func (fs *DefaultFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (fs *DefaultFS) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

func (fs *DefaultFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

func (fs *DefaultFS) WriteFile(name string, data []byte) error {
	return os.WriteFile(name, data, 0664)
}
