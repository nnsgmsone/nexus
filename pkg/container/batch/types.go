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

package batch

import (
	"bytes"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vfs"
)

// Batch represents a part of a relationship
//
//	(vecs)  - columns
type Batch struct {
	rows int32
	fs   vfs.FS
	// Vecs col data
	vecs []*vector.Vector
}

func (bat *Batch) String() string {
	var str string

	for i, vec := range bat.vecs {
		str += vec.String()
		if i != len(bat.vecs)-1 {
			str += "\n"
		}
	}
	return str
}

func (bat *Batch) Rows() int {
	return int(bat.rows)
}

func (bat *Batch) SetRows(rows int) {
	bat.rows = int32(rows)
	for _, vec := range bat.vecs {
		vec.SetLength(rows)
	}
}

func (bat *Batch) VectorCount() int {
	return len(bat.vecs)
}

func (bat *Batch) SetVector(pos int, vec *vector.Vector) {
	bat.vecs[pos] = vec
}

func (bat *Batch) GetVector(pos int) *vector.Vector {
	return bat.vecs[pos]
}

func (bat *Batch) GetVectorPosition(vec *vector.Vector) int {
	for i := range bat.vecs {
		if vec == bat.vecs[i] {
			return i
		}
	}
	return -1
}

func (bat *Batch) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	buf.Write(encoding.Encode(&bat.rows))
	count := int32(bat.VectorCount())
	buf.Write(encoding.Encode(&count))
	for _, vec := range bat.vecs {
		data, err := vec.MarshalBinary()
		if err != nil {
			return nil, err
		}
		size := int32(len(data))
		buf.Write(encoding.Encode(&size))
		buf.Write(data)
	}
	return buf.Bytes(), nil
}

func (bat *Batch) UnmarshalBinary(data []byte) error {
	bat.rows = encoding.Decode[int32](data[:4])
	data = data[4:]
	count := encoding.Decode[int32](data[:4])
	data = data[4:]
	bat.vecs = make([]*vector.Vector, count)
	for i := range bat.vecs {
		size := encoding.Decode[int32](data[:4])
		data = data[4:]
		vec, err := vector.New(vector.FLAT, types.New(types.T_bool), bat.fs)
		if err != nil {
			return err
		}
		if err = vec.UnmarshalBinary(data[:size]); err != nil {
			return err
		}
		bat.vecs[i] = vec
	}
	return nil
}
