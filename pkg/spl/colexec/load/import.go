package load

import (
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/engine"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

func New(paths []string,
	e engine.Engine, proc *process.Process) *ImportOp {
	return &ImportOp{
		e:     e,
		proc:  proc,
		paths: paths,
	}
}

func (o *ImportOp) Specialize() error {
	return nil
}

func (o *ImportOp) GetExecFunc() func(*pipeline.Message) ([]*pipeline.Message, int, error) {
	return o.Exec
}

func (o *ImportOp) Exec(_ *pipeline.Message) ([]*pipeline.Message, int, error) {
	vec, err := vector.New(vector.FLAT, &types.StringType, o.proc.FS())
	if err != nil {
		return nil, pipeline.END, err
	}
	for _, path := range o.paths {
		if err := loadData(vec, path, o.proc.FS()); err != nil {
			return nil, pipeline.END, err
		}
	}
	if err := o.e.Write(vec); err != nil {
		return nil, pipeline.END, err
	}
	return nil, pipeline.END, nil
}

func (o *ImportOp) Free() error {
	return nil
}

func loadData(vec *vector.Vector, path string, fs vfs.FS) error {
	st, err := fs.Stat(path)
	if err != nil {
		return err
	}
	if st.Size() < types.MaxInlineStringLength {
		data, err := fs.ReadFile(path)
		if err != nil {
			return err
		}
		return vector.AppendString(vec, data, false)
	}
	src, err := fs.Open(path)
	if err != nil {
		return err
	}
	defer src.Close()
	id := uuid.New()
	dst, err := fs.Open(id.String())
	if err != nil {
		return err
	}
	defer dst.Close()
	size, err := io.Copy(dst, src)
	if err != nil {
		return err
	}
	if size != int64(st.Size()) {
		return fmt.Errorf("size not match: %d-%d", size, st.Size())
	}
	if err := dst.Sync(); err != nil {
		return err
	}
	return vector.AppendStringUUID(vec, id, false, uint64(size))
}
