package noah

import (
	"bytes"
	"io"

	"github.com/google/uuid"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/encoding"
)

func (r *reader) Specialize() error {
	var err error

	r.vec, err = vector.New(vector.FLAT, &types.StringType, r.n.fs)
	return err
}

func (r *reader) Read(vec *vector.Vector, buf *bytes.Buffer) error {
	vec.Reset()
	if r.off == len(r.fileList) {
		return nil
	}
	id := uuid.UUID(r.fileList[r.off : r.off+uuidSize])
	fp, err := r.n.fs.Open(id.String())
	if err != nil {
		return err
	}
	size := encoding.Decode[uint64](r.fileList[r.off+uuidSize : r.off+fileEntrySize])
	buf.Reset()
	buf.Grow(int(size))
	n, err := fp.Read(buf.Bytes()[:size])
	if n != int(size) {
		if err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	if err := r.vec.UnmarshalBinary(buf.Bytes()[:size]); err != nil {
		return err
	}
	if err := vec.GetUnionFunction()(r.vec, nil); err != nil {
		return err
	}
	r.off += fileEntrySize
	return nil
}
