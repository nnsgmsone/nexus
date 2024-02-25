package noah

import (
	"github.com/google/uuid"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/defines"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/engine"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
	"github.com/nnsgmsone/wal"
)

func New(fs vfs.FS) (*noah, error) {
	var n noah
	var err error

	n.wal, err = wal.Open("noah.wal", 0664)
	if err != nil {
		return nil, err
	}
	n.fs = fs
	if err = n.recovery(); err != nil {
		return nil, err
	}
	return &n, nil
}

func (n *noah) Clean() error {
	var l logEntry

	l.typ = clean
	_, err := n.wal.Write(encodeLog(l))
	if err != nil {
		return err
	}
	n.fileList = n.fileList[:0]
	return nil
}

func (n *noah) Write(vec *vector.Vector) error {
	var l logEntry

	data, err := vec.MarshalBinary()
	if err != nil {
		return err
	}
	id := uuid.New()
	if err := n.fs.WriteFile(id.String(), data); err != nil {
		return err
	}
	size := uint64(len(data))
	fe := make([]byte, fileEntrySize)
	copy(fe[:16], id[:])
	copy(fe[16:], encoding.Encode[uint64](&size))
	l.typ = insert
	l.logData = fe
	if _, err := n.wal.Write(encodeLog(l)); err != nil {
		return err
	}
	n.fileList = append(n.fileList, fe...)
	return nil
}

func (n *noah) NewReader(proc *process.Process) (engine.Reader, error) {
	return &reader{
		n:        n,
		fileList: n.fileList,
	}, nil
}

func (n *noah) recovery() error {
	r, err := n.wal.NewReader(0)
	if err != nil {
		return err
	}
	for {
		idx, log, err := r.Next()
		if err != nil {
			return err
		}
		if idx == -1 {
			return nil
		}
		l := decodeLog(log)
		switch {
		case l.typ == insert:
			if len(l.logData) != fileEntrySize {
				return defines.ErrInvalidLog
			}
			n.fileList = append(n.fileList, l.logData...)
		case l.typ == clean:
			n.fileList = n.fileList[:0]
		default:
			return defines.ErrInvalidLog
		}
	}
}
