package agg

import (
	"bytes"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type min[T numeric] struct {
	// The minimum value
	vals []T
	// The number of elements seen
	cnts []int64
	typ  types.Type
	vec  *vector.Vector
	proc *process.Process
}

type boolMin struct {
	vals []bool
	cnts []int64
	typ  types.Type
	vec  *vector.Vector
	proc *process.Process
}

type strMin struct {
	vals [][]byte
	cnts []int64
	typ  types.Type
	vec  *vector.Vector
	proc *process.Process
}

func (a *min[T]) Load(vec *vector.Vector) error {
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

func (a *min[T]) Save() (*vector.Vector, error) {
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

func (a *min[T]) Specialize(proc *process.Process) error {
	var err error

	if a.vec, err = vector.New(vector.FLAT, &a.typ,
		proc.FS()); err != nil {
		return err
	}
	a.proc = proc
	return nil
}

func (a *min[T]) Grows(n int) {
	var v T

	for i := 0; i < n; i++ {
		a.vals = append(a.vals, v)
		a.cnts = append(a.cnts, 0)
	}
}

func (a *min[T]) BulkFill(grp uint32, vecs []*vector.Vector) error {
	vs := vector.GetColumnValue[T](vecs[0])
	for i := 0; i < vecs[0].Length(); i++ {
		if vecs[0].IsNull(i) {
			continue
		}
		if a.cnts[grp] == 0 || vs[i] < a.vals[grp] {
			a.vals[grp] = vs[i]
		}
		a.cnts[grp]++
	}
	return nil
}

func (a *min[T]) Fill(grps, sels []uint32, vecs []*vector.Vector) error {
	vs := vector.GetColumnValue[T](vecs[0])
	for i, g := range grps {
		if vecs[0].IsNull(int(sels[i])) {
			continue
		}
		if a.cnts[g] == 0 || vs[sels[i]] < a.vals[g] {
			a.vals[g] = vs[sels[i]]
		}
		a.cnts[g]++
	}
	return nil
}

func (a *min[T]) Merge(o Agg, agrps, bgrps []uint32) error {
	b := o.(*min[T])
	for i, g := range agrps {
		if a.cnts[g] == 0 || (b.cnts[bgrps[i]] > 0 && b.vals[bgrps[i]] < a.vals[g]) {
			a.vals[g] = b.vals[bgrps[i]]
		}
		a.cnts[g] += b.cnts[bgrps[i]]
	}
	return nil
}

func (a *min[T]) Eval() (*vector.Vector, error) {
	var dv T // the default value

	if err := a.vec.PreExtend(len(a.vals)); err != nil {
		return nil, err
	}
	a.vec.Reset()
	for i, v := range a.vals {
		if a.cnts[i] == 0 {
			if err := vector.Append(a.vec, dv, true); err != nil {
				return nil, err
			}
			continue
		}
		if err := vector.Append(a.vec, v, false); err != nil {
			return nil, err
		}
	}
	return a.vec, nil
}

func (a *boolMin) Load(vec *vector.Vector) error {
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
	vals := encoding.DecodeSlice[bool](data)
	a.vals = make([]bool, len(vals))
	copy(a.vals, vals)
	return nil
}

func (a *boolMin) Save() (*vector.Vector, error) {
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

func (a *boolMin) Specialize(proc *process.Process) error {
	var err error

	if a.vec, err = vector.New(vector.FLAT, &a.typ,
		proc.FS()); err != nil {
		return err
	}
	a.proc = proc
	return nil
}

func (a *boolMin) Grows(n int) {
	for i := 0; i < n; i++ {
		a.vals = append(a.vals, false)
		a.cnts = append(a.cnts, 0)
	}
}

func (a *boolMin) BulkFill(grp uint32, vecs []*vector.Vector) error {
	vs := vector.GetColumnValue[bool](vecs[0])
	for i := 0; i < vecs[0].Length(); i++ {
		if vecs[0].IsNull(i) {
			continue
		}
		if a.cnts[grp] == 0 || (!vs[i] && a.vals[grp]) {
			a.vals[grp] = vs[i]
		}
		a.cnts[grp]++
	}
	return nil
}

func (a *boolMin) Fill(grps, sels []uint32, vecs []*vector.Vector) error {
	vs := vector.GetColumnValue[bool](vecs[0])
	for i, g := range grps {
		if vecs[0].IsNull(int(sels[i])) {
			continue
		}
		if a.cnts[g] == 0 || (!vs[sels[i]] && a.vals[g]) {
			a.vals[g] = vs[sels[i]]
		}
		a.cnts[g]++
	}
	return nil
}

func (a *boolMin) Merge(o Agg, agrps, bgrps []uint32) error {
	b := o.(*boolMin)
	for i, g := range agrps {
		if a.cnts[g] == 0 || (b.cnts[bgrps[i]] > 0 && (!b.vals[bgrps[i]] && a.vals[g])) {
			a.vals[g] = b.vals[bgrps[i]]
		}
		a.cnts[g] += b.cnts[bgrps[i]]
	}
	return nil
}

func (a *boolMin) Eval() (*vector.Vector, error) {
	if err := a.vec.PreExtend(len(a.vals)); err != nil {
		return nil, err
	}
	a.vec.Reset()
	for i, v := range a.vals {
		if a.cnts[i] == 0 {
			if err := vector.Append(a.vec, false, true); err != nil {
				return nil, err
			}
			continue
		}
		if err := vector.Append(a.vec, v, false); err != nil {
			return nil, err
		}
	}
	return a.vec, nil
}

func (a *strMin) Load(vec *vector.Vector) error {
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
	a.cnts = make([]int64, len(cnts))
	copy(a.cnts, cnts)
	data = data[cnt*8:]
	strVec, err := vector.New(vector.FLAT, &types.StringType, a.proc.FS())
	if err != nil {
		return err
	}
	if err := strVec.UnmarshalBinary(data); err != nil {
		return err
	}
	vals := vector.GetColumnValue[types.String](strVec)
	a.vals = make([][]byte, strVec.Length())
	for i := 0; i < strVec.Length(); i++ {
		if strVec.IsNull(i) {
			continue
		}
		v, err := strVec.GetStringValue(vals[i])
		if err != nil {
			return err
		}
		a.vals[i] = append(a.vals[i][:0], v...)
	}
	return nil
}

func (a *strMin) Save() (*vector.Vector, error) {
	var buf bytes.Buffer

	vec, err := vector.New(vector.CONSTANT, &types.StringType, a.proc.FS())
	if err != nil {
		return nil, err
	}
	cnt := uint64(len(a.vals))
	buf.Write(encoding.Encode(&a.typ))
	buf.Write(encoding.Encode(&cnt))
	buf.Write(encoding.EncodeSlice(a.cnts))
	strVec, err := vector.New(vector.FLAT, &types.StringType, a.proc.FS())
	if err != nil {
		return nil, err
	}
	for _, v := range a.vals {
		if err := vector.AppendString(strVec, v, false); err != nil {
			return nil, err
		}
	}
	data, err := strVec.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(data)
	if err := vector.AppendString(vec, buf.Bytes(), false); err != nil {
		return nil, err
	}
	return vec, nil
}

func (a *strMin) Specialize(proc *process.Process) error {
	var err error

	if a.vec, err = vector.New(vector.FLAT, &a.typ,
		proc.FS()); err != nil {
		return err
	}
	a.proc = proc
	return nil
}

func (a *strMin) Grows(n int) {
	for i := 0; i < n; i++ {
		a.vals = append(a.vals, []byte{})
		a.cnts = append(a.cnts, 0)
	}
}

func (a *strMin) BulkFill(grp uint32, vecs []*vector.Vector) error {
	vs := vector.GetColumnValue[types.String](vecs[0])
	for i := 0; i < vecs[0].Length(); i++ {
		if vecs[0].IsNull(i) {
			continue
		}
		v, err := vecs[0].GetStringValue(vs[i])
		if err != nil {
			return err
		}
		if a.cnts[grp] == 0 || bytes.Compare(v, a.vals[grp]) < 0 {
			a.vals[grp] = append(a.vals[grp][:0], v...)
		}
		a.cnts[grp]++
	}
	return nil
}

func (a *strMin) Fill(grps, sels []uint32, vecs []*vector.Vector) error {
	vs := vector.GetColumnValue[types.String](vecs[0])
	for i, g := range grps {
		if vecs[0].IsNull(int(sels[i])) {
			continue
		}
		v, err := vecs[0].GetStringValue(vs[sels[i]])
		if err != nil {
			return err
		}
		if a.cnts[g] == 0 || bytes.Compare(v, a.vals[g]) < 0 {
			a.vals[g] = append(a.vals[g][:0], v...)
		}
		a.cnts[g]++
	}
	return nil
}

func (a *strMin) Merge(o Agg, agrps, bgrps []uint32) error {
	b := o.(*strMin)
	for i, g := range agrps {
		if a.cnts[g] == 0 || (b.cnts[bgrps[i]] > 0 && bytes.Compare(b.vals[bgrps[i]], a.vals[g]) < 0) {
			a.vals[g] = append(a.vals[g][:0], b.vals[bgrps[i]]...)
		}
		a.cnts[g] += b.cnts[bgrps[i]]
	}
	return nil
}

func (a *strMin) Eval() (*vector.Vector, error) {
	if err := a.vec.PreExtend(len(a.vals)); err != nil {
		return nil, err
	}
	a.vec.Reset()
	for i, v := range a.vals {
		if a.cnts[i] == 0 {
			if err := vector.AppendString(a.vec, nil, true); err != nil {
				return nil, err
			}
			continue
		}
		if err := vector.AppendString(a.vec, v, false); err != nil {
			return nil, err
		}
	}
	return a.vec, nil
}
