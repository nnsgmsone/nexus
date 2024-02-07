// Copyright (C) 2021 nexus.
//
// This file is part of nexus
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package vector

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/nnsgmsone/nexus/pkg/container/bitmap"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/mheap"
)

func New(class int, typ *types.Type, fs vfs.FS) (*Vector, error) {
	var vec Vector

	nsp, err := bitmap.New(0)
	if err != nil {
		return nil, err
	}
	vec.fs = fs
	vec.typ = *typ
	vec.nsp = nsp
	vec.length = 0
	vec.area = nil
	vec.class = class
	return &vec, nil
}

func GetColumnValue[T any](v *Vector) []T {
	return getColumnValue[T](v)
}

func (vec *Vector) Dup() *Vector {
	rvec, _ := New(FLAT, &vec.typ, vec.fs)
	rvec.PreExtend(vec.length)
	rvec.GetUnionFunction()(vec, nil)
	return rvec
}

// PreExtend use to expand the capacity of the vector
func (vec *Vector) PreExtend(rows int) error {
	if vec.class == CONSTANT {
		return nil
	}
	switch vec.typ.Oid() {
	case types.T_bool:
		return extend[bool](vec, rows)
	case types.T_int64:
		return extend[int64](vec, rows)
	case types.T_float64:
		return extend[float64](vec, rows)
	case types.T_string:
		return extend[types.String](vec, rows)
	default:
		return errors.New(fmt.Sprintf("unexpect type %s for function vector.PreExtend", &vec.typ))
	}
}

func (vec *Vector) GetReader(str types.String) (io.ReadCloser, error) {
	data, uuid, _ := str.GetString(vec.area)
	if data != nil {
		return &readCloser{bytes.NewReader(data)}, nil
	}
	fp, err := vec.fs.Open(uuid.String())
	return fp, err
}

func GetShrinkFunction(typ types.Type) func(*Vector, []uint32) error {
	switch typ.Oid() {
	case types.T_bool:
		return func(vec *Vector, sels []uint32) error {
			return shrinkFixed[bool](vec, sels)
		}
	case types.T_int64:
		return func(vec *Vector, sels []uint32) error {
			return shrinkFixed[int64](vec, sels)
		}
	case types.T_float64:
		return func(vec *Vector, sels []uint32) error {
			return shrinkFixed[float64](vec, sels)
		}
	case types.T_string:
		return func(vec *Vector, sels []uint32) error {
			return shrinkFixed[types.String](vec, sels)
		}
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.GetShrinkFunction", typ.String()))
	}
}

func (v *Vector) GetUnionFunction() func(*Vector, []uint32) error {
	switch v.typ.Oid() {
	case types.T_bool:
		return func(w *Vector, sels []uint32) error {
			if w.IsConst() {
				return constUnionFixed[bool](v, w, sels)
			}
			return unionFixed[bool](v, w, sels)
		}
	case types.T_int64:
		return func(w *Vector, sels []uint32) error {
			if w.IsConst() {
				return constUnionFixed[int64](v, w, sels)
			}
			return unionFixed[int64](v, w, sels)
		}
	case types.T_float64:
		return func(w *Vector, sels []uint32) error {
			if w.IsConst() {
				return constUnionFixed[float64](v, w, sels)
			}
			return unionFixed[float64](v, w, sels)
		}
	case types.T_string:
		return func(w *Vector, sels []uint32) error {
			ws := GetColumnValue[types.String](w)
			if w.IsConst() {
				length := len(sels)
				if length == 0 {
					length = w.Length()
				}
				isNull := w.nsp.Contains(uint64(0))
				str, uuid, size := ws[0].GetString(w.area)
				if str == nil {
					for i := 0; i < length; i++ {
						if err := AppendStringUUID(v, uuid, isNull, size); err != nil {
							return err
						}
					}
				} else {
					for i := 0; i < length; i++ {
						if err := AppendString(v, str, isNull); err != nil {
							return err
						}
					}
				}
				return nil
			}
			if len(sels) == 0 {
				for i := 0; i < w.Length(); i++ {
					str, uuid, size := ws[i].GetString(w.area)
					if str == nil {
						if err := AppendStringUUID(v, uuid, w.nsp.Contains(uint64(i)), size); err != nil {
							return err
						}
					} else {
						if err := AppendString(v, str, w.nsp.Contains(uint64(i))); err != nil {
							return err
						}
					}
				}
			} else {
				for _, sel := range sels {
					str, uuid, size := ws[sel].GetString(w.area)
					if str == nil {
						if err := AppendStringUUID(v, uuid, w.nsp.Contains(uint64(sel)), size); err != nil {
							return err
						}
					} else {
						if err := AppendString(v, str, w.nsp.Contains(uint64(sel))); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.GetUnionFunction", &v.typ))
	}
}

func Append[T any](v *Vector, w T, isNull bool) error {
	return appendOne(v, w, isNull)
}

func AppendString(v *Vector, w []byte, isNull bool) error {
	return appendOneBytes(v, w, isNull)
}

func AppendStringUUID(v *Vector, uuid uuid.UUID, isNull bool, size uint64) error {
	return appendOneUUID(v, uuid, isNull, size)
}

func AppendList[T any](v *Vector, ws []T, nsp *bitmap.Bitmap) error {
	return appendList(v, ws, nsp)
}

func AppendStringList(v *Vector, ws [][]byte, nsp *bitmap.Bitmap) error {
	return appendStringList(v, ws, nsp)
}

func appendOne[T any](v *Vector, w T, isNull bool) error {
	if err := extend[T](v, 1); err != nil {
		return err
	}
	length := v.length
	v.length++
	col := getColumnValue[T](v)
	if isNull {
		v.nsp.Add(uint64(length))
	} else {
		col[length] = w
	}
	return nil
}

func appendOneBytes(v *Vector, bs []byte, isNull bool) error {
	var err error
	var str types.String

	if isNull {
		return appendOne(v, str, true)
	} else {
		if v.area, err = (&str).SetString(bs, v.area, v.fs); err != nil {
			return err
		}
		return appendOne(v, str, false)
	}
}

func appendOneUUID(v *Vector, uuid uuid.UUID, isNull bool, size uint64) error {
	var str types.String

	if isNull {
		return appendOne(v, str, true)
	} else {
		v.area = (&str).SetStringUUID(uuid, v.area, size)
		return appendOne(v, str, false)
	}
}

func appendList[T any](v *Vector, ws []T, nsp *bitmap.Bitmap) error {
	if err := extend[T](v, len(ws)); err != nil {
		return err
	}
	length := v.length
	v.length += len(ws)
	col := getColumnValue[T](v)
	for i, w := range ws {
		if nsp != nil && nsp.Contains(uint64(i)) {
			v.nsp.Add(uint64(i + length))
		} else {
			col[length+i] = w
		}
	}
	return nil
}

func appendStringList(v *Vector, ws [][]byte, nsp *bitmap.Bitmap) error {
	var err error
	var str types.String

	if err := extend[types.String](v, len(ws)); err != nil {
		return err
	}
	length := v.length
	v.length += len(ws)
	col := getColumnValue[types.String](v)
	for i, w := range ws {
		if nsp != nil && nsp.Contains(uint64(i)) {
			v.nsp.Add(uint64(i + length))
		} else {
			if v.area, err = (&str).SetString(w, v.area, v.fs); err != nil {
				return err
			}
			col[length+i] = str
		}
	}
	return nil
}

func extend[T any](v *Vector, rows int) error {
	sz := v.typ.Size()
	if length := v.length + rows; length >= cap(v.data)/sz {
		v.data = mheap.Realloc(v.data, int64(length*sz))
	}
	return nil
}

func unionFixed[T any](v, w *Vector, sels []uint32) error {
	ws := GetColumnValue[T](w)
	if len(sels) == 0 {
		for i := 0; i < w.Length(); i++ {
			if err := Append(v, ws[i], w.nsp.Contains((uint64(i)))); err != nil {
				return err
			}
		}
	} else {
		for _, sel := range sels {
			if err := Append(v, ws[sel], w.nsp.Contains(uint64(sel))); err != nil {
				return err
			}
		}
	}
	return nil
}

func constUnionFixed[T any](v, w *Vector, sels []uint32) error {
	ws := GetColumnValue[T](w)
	length := len(sels)
	if length == 0 {
		length = w.Length()
	}
	for i := 0; i < length; i++ {
		if err := Append(v, ws[0], w.nsp.Contains((uint64(0)))); err != nil {
			return err
		}
	}
	return nil
}

func shrinkFixed[T any](v *Vector, sels []uint32) error {
	if v.class != CONSTANT {
		if err := v.nsp.Shrink(sels); err != nil {
			return err
		}
		vs := getColumnValue[T](v)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
	}
	v.length = len(sels)
	return nil
}

func getColumnValue[T any](v *Vector) []T {
	if len(v.data) == 0 {
		return nil
	}
	if v.class == CONSTANT {
		return encoding.DecodeSlice[T](v.data)[:1]
	}
	return encoding.DecodeSlice[T](v.data)[:v.length]
}

func vecToString[T any](v *Vector) string {
	col := getColumnValue[T](v)
	if len(col) == 1 {
		if v.nsp.Contains(0) {
			return "null"
		} else {
			return fmt.Sprintf("%v", col[0])
		}
	}
	return fmt.Sprintf("%v-%s", col, v.nsp)
}

func vecValToString[T any](v *Vector, row int) string {
	col := getColumnValue[T](v)
	if len(col) == 1 {
		if v.nsp.Contains(0) {
			return "null"
		} else {
			return fmt.Sprintf("%v", col[0])
		}
	}
	if v.nsp.Contains(uint64(row)) {
		return "null"
	}
	return fmt.Sprintf("%v", col[row])
}
