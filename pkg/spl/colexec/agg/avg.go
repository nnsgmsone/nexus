package agg

import (
	"bytes"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/encoding"

	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type avg[T numeric] struct {
	vals []T
	cnts []int64
	typ  types.Type
	vec  *vector.Vector
	proc *process.Process
}

func (a *avg[T]) Load(vec *vector.Vector) error {
	var err error
	var data []byte

	strs := vector.GetColumnValue[types.String](vec)
	if data, err = vec.GetStringValue(strs[0]); err != nil {
		return err
	}
	a.typ = encoding.Decode[types.Type](data[:types.TypeSize])
	data = data[types.TypeSize:]
	cnt := encoding.Decode[uint64](data[:8])
	data = data[8:]
	cnts := encoding.DecodeSlice[int64](data[:cnt*8])
	data = data[cnt*8:]
	a.cnts = make([]int64, len(cnts))
	copy(a.cnts, cnts)
	vals := encoding.DecodeSlice[T](data)
	a.vals = make([]T, len(vals))
	copy(a.vals, vals)
	return nil
}

func (a *avg[T]) Save() (*vector.Vector, error) {
	var buf bytes.Buffer

	vec, err := vector.New(vector.CONSTANT, &types.StringType, a.proc.FS())
	if err != nil {
		return nil, err
	}
	cnt := uint64(len(a.vals))
	buf.Write(encoding.Encode(&a.typ))
	buf.Write(encoding.Encode(&cnt))
	buf.Write(encoding.EncodeSlice(a.cnts))
	buf.Write(encoding.EncodeSlice(a.vals))
	if err := vector.AppendString(vec, buf.Bytes(), false); err != nil {
		return nil, err
	}
	return vec, nil
}

func (a *avg[T]) Specialize(proc *process.Process) error {
	var err error

	if a.vec, err = vector.New(vector.FLAT, &a.typ,
		proc.FS()); err != nil {
		return err
	}
	a.proc = proc
	return nil
}

func (a *avg[T]) Grows(n int) {
	for i := 0; i < n; i++ {
		a.vals = append(a.vals, 0)
		a.cnts = append(a.cnts, 0)
	}
}

func (a *avg[T]) BulkFill(grp uint32, vecs []*vector.Vector) error {
	vs := vector.GetColumnValue[T](vecs[0])
	for i := 0; i < vecs[0].Length(); i++ {
		if vecs[0].IsNull(i) {
			continue
		}
		a.vals[grp] += vs[i]
		a.cnts[grp]++
	}
	return nil
}

func (a *avg[T]) Fill(grps, sels []uint32, vecs []*vector.Vector) error {
	vs := vector.GetColumnValue[T](vecs[0])
	for i, g := range grps {
		if vecs[0].IsNull(int(sels[i])) {
			continue
		}
		a.vals[g] += vs[sels[i]]
		a.cnts[g]++
	}
	return nil
}

func (a *avg[T]) Merge(o Agg, agrps, bgrps []uint32) error {
	b := o.(*avg[T])
	for i, g := range agrps {
		a.vals[g] += b.vals[bgrps[i]]
		a.cnts[g] += b.cnts[bgrps[i]]
	}
	return nil
}

func (a *avg[T]) Eval() (*vector.Vector, error) {
	if err := a.vec.PreExtend(len(a.vals)); err != nil {
		return nil, err
	}
	a.vec.Reset()
	for i, v := range a.vals {
		if a.cnts[i] == 0 {
			if err := vector.Append(a.vec, float64(0), true); err != nil {
				return nil, err
			}
			continue
		}
		if err := vector.Append(a.vec, float64(v)/float64(a.cnts[i]),
			false); err != nil {
			return nil, err
		}
	}
	return a.vec, nil
}
