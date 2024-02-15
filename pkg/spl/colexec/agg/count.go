package agg

import (
	"bytes"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type count struct {
	cnts []int64
	typ  types.Type
	vec  *vector.Vector
	proc *process.Process
}

func (a *count) Load(vec *vector.Vector) error {
	var err error
	var data []byte

	vec.SetLength(1)
	strs := vector.GetColumnValue[types.String](vec)
	if data, err = vec.GetStringValue(strs[0]); err != nil {
		return err
	}
	a.typ = encoding.Decode[types.Type](data[:types.TypeSize])
	data = data[types.TypeSize:]
	cnts := encoding.DecodeSlice[int64](data)
	a.cnts = make([]int64, len(cnts))
	copy(a.cnts, cnts)
	return nil
}

func (a *count) Save() (*vector.Vector, error) {
	var buf bytes.Buffer

	vec, err := vector.New(vector.CONSTANT, &types.StringType, a.proc.FS())
	if err != nil {
		return nil, err
	}
	buf.Write(encoding.Encode(&a.typ))
	buf.Write(encoding.EncodeSlice(a.cnts))
	if err := vector.AppendString(vec, buf.Bytes(), false); err != nil {
		return nil, err
	}
	return vec, nil
}

func (a *count) Specialize(proc *process.Process) error {
	var err error

	if a.vec, err = vector.New(vector.FLAT, &a.typ,
		proc.FS()); err != nil {
		return err
	}
	a.proc = proc
	return nil
}

func (a *count) Grows(n int) {
	for i := 0; i < n; i++ {
		a.cnts = append(a.cnts, 0)
	}
}

func (a *count) BulkFill(grp uint32, vecs []*vector.Vector) error {
	for i := 0; i < vecs[0].Length(); i++ {
		if vecs[0].IsNull(i) {
			continue
		}
		a.cnts[grp]++
	}
	return nil
}

func (a *count) Fill(grps, sels []uint32, vecs []*vector.Vector) error {
	for i, g := range grps {
		if vecs[0].IsNull(int(sels[i])) {
			continue
		}
		a.cnts[g]++
	}
	return nil
}

func (a *count) Merge(o Agg, agrps, bgrps []uint32) error {
	b := o.(*count)
	for i, g := range agrps {
		a.cnts[g] += b.cnts[bgrps[i]]
	}
	return nil
}

func (a *count) Eval() (*vector.Vector, error) {
	if err := a.vec.PreExtend(len(a.cnts)); err != nil {
		return nil, err
	}
	a.vec.Reset()
	for _, v := range a.cnts {
		if err := vector.Append(a.vec, v, false); err != nil {
			return nil, err
		}
	}
	return a.vec, nil
}
