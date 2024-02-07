package process

import (
	"github.com/google/uuid"
	"github.com/nnsgmsone/nexus/pkg/vfs"
)

// Process contains context used in query execution
type Process struct {
	id uuid.UUID
	// unix timestamp
	unixTime int64
	fs       vfs.FS
}

func (proc *Process) ID() uuid.UUID {
	return proc.id
}

func (proc *Process) UnixTime() int64 {
	return proc.unixTime
}

func (proc *Process) FS() vfs.FS {
	return proc.fs
}
