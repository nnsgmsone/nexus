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
	"fmt"
	"io"
	"unsafe"

	"github.com/google/uuid"
	"github.com/nnsgmsone/nexus/pkg/container/bitmap"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/mheap"
)

const (
	FLAT     = iota // flat vector represent a uncompressed vector
	CONSTANT        // const vector
)

// Vector represent a column
type Vector struct {
	class  int
	length int

	typ types.Type
	nsp *bitmap.Bitmap

	fs vfs.FS

	area []byte

	data []byte
}

type readCloser struct {
	r io.Reader
}

func (rc *readCloser) Read(p []byte) (int, error) {
	return rc.r.Read(p)
}

func (rc *readCloser) Close() error {
	return nil
}

func (v *Vector) Reset() {
	v.length = 0
	v.area = v.area[:0]
	v.nsp.Reset()
}

func (v *Vector) TypeEqual(w *Vector) bool {
	return v.typ.Equal(&w.typ)
}

func (v *Vector) GetString(str types.String) ([]byte, uuid.UUID, uint64) {
	return str.GetString(v.area)
}

func (v *Vector) SetStringValue(str types.String, b []byte) error {
	var err error

	v.area, err = str.SetString(b, v.area, v.fs)
	return err
}

func (v *Vector) GetStringValue(str types.String) ([]byte, error) {
	b, uuid, _ := v.GetString(str)
	if b != nil {
		return b, nil
	}
	return v.fs.ReadFile(uuid.String())
}

func (v *Vector) Length() int {
	return v.length
}

func (v *Vector) SetLength(n int) {
	v.length = n
}

func (v *Vector) GetType() types.Type {
	return v.typ
}

func (v *Vector) IsNull(i int) bool {
	if v.class == CONSTANT {
		return v.nsp.Contains(0)
	}
	return v.nsp.Contains(uint64(i))
}

func (v *Vector) SetNull(i int) {
	if v.class == CONSTANT {
		v.nsp.Add(0)
	}
	v.nsp.Add(uint64(i))
}

func (v *Vector) SetNotNull(i int) {
	if v.class == CONSTANT {
		v.nsp.Remove(0)
	}
	v.nsp.Remove(uint64(i))
}

func (v *Vector) IsConst() bool {
	return v.class == CONSTANT
}

func (v *Vector) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteByte(uint8(v.class))
	{ // write length
		length := uint64(v.length)
		buf.Write(encoding.Encode(&length))
	}
	{ // write type
		data := encoding.Encode(&v.typ)
		buf.Write(data)
	}
	{
		if v.nsp != nil {
			data, err := v.nsp.Marshal()
			if err != nil {
				return nil, err
			}
			length := uint64(len(data))
			buf.Write(encoding.Encode(&length))
			if len(data) > 0 {
				buf.Write(data)
			}
		} else {
			length := uint64(0)
			buf.Write(encoding.Encode(&length))
		}
	}
	{ // write colLen, col
		data := v.data
		length := uint32(v.length * v.typ.Size())
		if len(data[:length]) > 0 {
			buf.Write(data[:length])
		}
	}
	{ // write areaLen, area
		length := uint64(len(v.area))
		buf.Write(encoding.Encode(&length))
		if len(v.area) > 0 {
			buf.Write(v.area)
		}
	}
	return buf.Bytes(), nil
}

func (v *Vector) UnmarshalBinary(data []byte) error {
	{ // read class
		v.class = int(data[0])
		data = data[1:]
	}
	{ // read length
		v.length = int(encoding.Decode[uint64](data[:8]))
		data = data[8:]
	}
	{ // read typ
		typ := encoding.Decode[types.Type](data[:types.TypeSize])
		v.typ = typ
		data = data[types.TypeSize:]
	}
	{ // read nsp
		size := encoding.Decode[uint64](data)
		data = data[8:]
		if size > 0 {
			ndata := make([]byte, len(data))
			copy(ndata, data[:size])
			if err := v.nsp.Unmarshal(ndata); err != nil {
				return err
			}
			data = data[size:]
		}
	}
	{ // read col
		length := v.length * v.typ.Size()
		if length > 0 {
			v.data = data[:length]
			data = data[length:]
		}
	}
	{ // read area
		length := encoding.Decode[uint64](data)
		data = data[8:]
		if length > 0 {
			ndata := mheap.Alloc(int64(length))
			copy(ndata, data[:length])
			v.area = ndata
			data = data[:length]
		}
	}
	return nil
}

func (v *Vector) GetValueString(row int) string {
	switch v.typ.Oid() {
	case types.T_bool:
		return vecValToString[bool](v, row)
	case types.T_int64:
		return vecValToString[int64](v, row)
	case types.T_float64:
		return vecValToString[float64](v, row)
	case types.T_string:
		col := getColumnValue[types.String](v)
		if v.length == 1 {
			if v.nsp.Contains(0) {
				return "null"
			}
			str, uuid, size := v.GetString(col[0])
			if str == nil {
				return fmt.Sprintf("<in file %x>", uuid)
			}
			if size == 0 {
				return ""
			}
			return unsafe.String(&str[0], len(str))
		}
		if v.nsp.Contains(uint64(row)) {
			return "null"
		}
		str, uuid, size := v.GetString(col[row])
		if str == nil {
			return fmt.Sprintf("<in file %x>", uuid)
		}
		if size == 0 {
			return ""
		}
		return unsafe.String(&str[0], len(str))
	default:
		panic("vec to string unknown types.")
	}

}

func (v *Vector) String() string {
	switch v.typ.Oid() {
	case types.T_bool:
		return vecToString[bool](v)
	case types.T_int64:
		return vecToString[int64](v)
	case types.T_float64:
		return vecToString[float64](v)
	case types.T_string:
		col := getColumnValue[types.String](v)
		vs := make([]string, len(col))
		for i := range col {
			str, uuid, _ := v.GetString(col[i])
			if str == nil {
				vs[i] = fmt.Sprintf("<in file %x>", uuid)
			} else {
				vs[i] = unsafe.String(&str[0], len(str))
			}
		}
		if len(vs) == 1 {
			if v.nsp.Contains(0) {
				return "null"
			} else {
				return vs[0]
			}
		}
		return fmt.Sprintf("%v-%s", vs, v.nsp)
	default:
		panic("vec to string unknown types.")
	}
}
