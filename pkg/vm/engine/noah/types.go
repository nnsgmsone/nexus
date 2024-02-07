package noah

import (
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/wal"
)

const (
	insert = iota
	clean
)

const (
	uuidSize      = 16
	fileEntrySize = 24 // 24 = 16 + 8; 16 = uuid size, 8 = uint64 size
)

type noah struct {
	fs       vfs.FS
	wal      *wal.Wal
	fileList []byte
}

type logEntry struct {
	typ     uint64
	logData []byte
}

type reader struct {
	off      int
	n        *noah
	fileList []byte
	vec      *vector.Vector
}
