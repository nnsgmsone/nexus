package process

import (
	"time"

	"github.com/google/uuid"
	"github.com/nnsgmsone/nexus/pkg/vfs"
)

// New creates a new Process.
// A process stores the execution context.
func New(fs vfs.FS) *Process {
	return &Process{
		fs:       fs,
		id:       uuid.New(),
		unixTime: time.Now().Unix(),
	}
}
