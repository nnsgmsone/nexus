package scan

import (
	"bytes"
	"io"
	"os"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/defines"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vm/engine"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
	lua "github.com/yuin/gopher-lua"
)

func New(r engine.Reader, lua string, cols []int, proc *process.Process) *ScanOp {
	return &ScanOp{
		r:    r,
		lua:  lua,
		cols: cols,
		proc: proc,
	}
}

func (o *ScanOp) Specialize() error {
	fs := o.proc.FS()
	o.bat = batch.New(len(o.cols), fs)
	o.buf = new(bytes.Buffer)
	o.buf.Grow(DefaultBufferSize)
	vec, err := vector.New(vector.FLAT, &types.StringType, fs)
	if err != nil {
		return err
	}
	if err := vec.PreExtend(defines.DefaultRows); err != nil {
		return err
	}
	o.vec = vec
	for i := range o.cols {
		vec, err := vector.New(vector.FLAT, &types.StringType, fs)
		if err != nil {
			return err
		}
		if err := vec.PreExtend(defines.DefaultRows); err != nil {
			return err
		}
		o.bat.SetVector(i, vec)
	}
	if err := o.r.Specialize(); err != nil {
		return err
	}
	o.msgs = make([]*pipeline.Message, 1)
	o.msgs[0] = pipeline.NewMessage(pipeline.DATA, pipeline.NEEDDUP, -1, nil)
	o.state.l = lua.NewState(lua.Options{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Stdin:  o,
	})
	o.state.l.SetGlobal("writeResult", o.state.l.NewFunction(func(L *lua.LState) int {
		if L.Get(1).Type() == lua.LTNil {
			o.state.ch <- nil
			return 0
		}
		result := L.CheckTable(1)
		o.state.ch <- result
		return 0
	}))
	o.state.ch = make(chan *lua.LTable)
	go func() {
		err := o.state.l.DoString(o.lua)
		if err != nil {
			panic(err)
		}
	}()
	return nil
}

func (o *ScanOp) GetExecFunc() func(*pipeline.Message) ([]*pipeline.Message, int, error) {
	return o.Exec
}

func (o *ScanOp) Exec(msg *pipeline.Message) ([]*pipeline.Message, int, error) {
	if msg != nil && msg.IsStopMessage() {
		return nil, pipeline.END, nil
	}
	tbl, ok := <-o.state.ch
	if !ok || tbl == nil {
		return nil, pipeline.END, nil
	}
	o.bat.Reset()
	for i := range o.cols {
		vec := o.bat.GetVector(i)
		col := tbl.RawGetInt(o.cols[i] + 1).(*lua.LTable)
		for j := 1; j < col.Len()+1; j++ {
			if err := vector.AppendString(vec, encoding.String2Bytes(col.RawGetInt(j).String()), false); err != nil {
				return nil, pipeline.END, err
			}
		}

	}
	o.bat.SetRows(o.bat.GetVector(0).Length())
	o.msgs[0].Reset(pipeline.DATA, pipeline.NEEDDUP, -1, o.bat)
	return o.msgs, pipeline.EVAL, nil
}

func (o *ScanOp) Free() error {
	o.bat = nil
	o.buf = nil
	o.msgs = nil
	o.state.l.Close()
	return nil
}

func (o *ScanOp) Read(p []byte) (int, error) {
	for {
		if o.rc == nil {
			ok, err := o.nextReader()
			if err != nil {
				return 0, err
			}
			if ok {
				if err := o.r.Read(o.vec, o.buf); err != nil {
					return 0, err
				}
				if o.vec.Length() == 0 {
					return 0, io.EOF
				}
				o.strs = vector.GetColumnValue[types.String](o.vec)
				rc, err := o.vec.GetReader(o.strs[0])
				if err != nil {
					return 0, err
				}
				o.strs = o.strs[1:]
				o.rc = rc
			}
		}
		n, err := o.rc.Read(p)
		if n == 0 && err == io.EOF {
			o.rc.Close()
			o.rc = nil
			continue
		}
		if err != nil {
			return 0, err
		}
		return n, err
	}
}

func (o *ScanOp) Close() error {
	return nil
}

// return true if next reader is empty
func (o *ScanOp) nextReader() (bool, error) {
	var err error

	if len(o.strs) == 0 {
		return true, nil
	}
	o.rc, err = o.vec.GetReader(o.strs[0])
	if err != nil {
		return true, err
	}
	o.strs = o.strs[1:]
	return false, nil
}
