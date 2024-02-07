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

package testutil

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vfs"
)

func NewBatch(ts []types.Type, n int, random bool) *batch.Batch {
	bat := batch.New(len(ts), vfs.NewMemFS())
	for i := 0; i < bat.VectorCount(); i++ {
		bat.SetVector(i, NewVector(n, ts[i], random))
	}
	bat.SetRows(n)
	return bat
}

func NewVector(n int, typ types.Type, random bool) *vector.Vector {
	switch typ.Oid() {
	case types.T_bool:
		return NewBoolVector(n, typ, random)
	case types.T_int64:
		return NewLongVector(n, typ, random)
	case types.T_float64:
		return NewDoubleVector(n, typ, random)
	case types.T_string:
		return NewStringVector(n, typ, random)
	default:
		panic(fmt.Errorf("unsupport type '%v", typ))
	}
}

func NewBoolVector(n int, typ types.Type, random bool) *vector.Vector {
	vec, err := vector.New(vector.FLAT, &typ, vfs.NewMemFS())
	if err != nil {
		panic(err)
	}
	appendBoolVector(n, vec, random)
	return vec
}

func NewLongVector(n int, typ types.Type, random bool) *vector.Vector {
	vec, err := vector.New(vector.FLAT, &typ, vfs.NewMemFS())
	if err != nil {
		panic(err)
	}
	appendLongVector(n, vec, random)
	return vec
}

func NewDoubleVector(n int, typ types.Type, random bool) *vector.Vector {
	vec, err := vector.New(vector.FLAT, &typ, vfs.NewMemFS())
	if err != nil {
		panic(err)
	}
	appendDoubleVector(n, vec, random)
	return vec
}

func NewStringVector(n int, typ types.Type, random bool) *vector.Vector {
	vec, err := vector.New(vector.FLAT, &typ, vfs.NewMemFS())
	if err != nil {
		panic(err)
	}
	appendStringVector(n, vec, random)
	return vec
}

func appendBoolVector(n int, vec *vector.Vector, random bool) {
	for i := 0; i < n; i++ {
		v := i%2 == 0
		if random {
			v = rand.Int()%2 == 0
		}
		if err := vector.Append(vec, bool(v), false); err != nil {
			panic(err)
		}
	}
}

func appendLongVector(n int, vec *vector.Vector, random bool) {
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.Append(vec, int64(v), false); err != nil {
			panic(err)
		}
	}
}

func appendDoubleVector(n int, vec *vector.Vector, random bool) {
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vector.Append(vec, float64(v), false); err != nil {
			panic(err)
		}
	}
}

func appendStringVector(n int, vec *vector.Vector, random bool) {
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		str := strconv.Itoa(v)
		if err := vector.AppendString(vec, encoding.String2Bytes(str), false); err != nil {
			panic(err)
		}
	}
}
