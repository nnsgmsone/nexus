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
	"testing"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/stretchr/testify/require"
)

const (
	benchRows = 1024
)

func TestLength(t *testing.T) {
	fs := vfs.NewMemFS()
	vec, err := New(FLAT, types.New(types.T_int64), fs)
	require.NoError(t, err)
	err = AppendList(vec, []int64{0, 1, 2}, nil)
	require.NoError(t, err)
	require.Equal(t, 3, vec.Length())
	vec.SetLength(2)
	require.Equal(t, 2, vec.Length())
}

func TestAppend(t *testing.T) {
	fs := vfs.NewMemFS()
	vec, err := New(FLAT, types.New(types.T_int64), fs)
	require.NoError(t, err)
	err = Append(vec, int64(0), false)
	require.NoError(t, err)
	err = Append(vec, int64(0), true)
	require.NoError(t, err)
	err = AppendList(vec, []int64{0, 1, 2}, nil)
	require.NoError(t, err)
}

func TestAppendBytes(t *testing.T) {
	fs := vfs.NewMemFS()
	vec, err := New(FLAT, types.New(types.T_string), fs)
	require.NoError(t, err)
	err = AppendString(vec, []byte("x"), false)
	require.NoError(t, err)
	err = AppendString(vec, nil, true)
	require.NoError(t, err)
	err = AppendStringList(vec, [][]byte{[]byte("x"), []byte("y")}, nil)
	require.NoError(t, err)
	vs := GetColumnValue[types.String](vec)
	for _, v := range vs {
		vec.GetString(v)
	}
}

func TestAppendLargeBytes(t *testing.T) {
	fs := vfs.NewMemFS()
	vec, err := New(FLAT, types.New(types.T_string), fs)
	require.NoError(t, err)
	data := make([]byte, types.MaxInlineStringLength+1)
	data[0] = 'x'
	err = AppendString(vec, data, false)
	require.NoError(t, err)
	vs := GetColumnValue[types.String](vec)
	for _, v := range vs {
		b, err := vec.GetStringValue(v)
		require.NoError(t, err)
		require.Equal(t, data, b)
	}
}

func TestShrink(t *testing.T) {
	fs := vfs.NewMemFS()
	v, err := New(FLAT, types.New(types.T_int64), fs)
	require.NoError(t, err)
	err = AppendList(v, []int64{0, 1, 2}, nil)
	require.NoError(t, err)
	sf := GetShrinkFunction(v.GetType())
	sf(v, []uint32{1})
	require.Equal(t, []int64{1}, GetColumnValue[int64](v))
}

func TestUnion(t *testing.T) {
	fs := vfs.NewMemFS()
	{
		v, err := New(FLAT, types.New(types.T_int64), fs)
		require.NoError(t, err)
		err = AppendList(v, []int64{0, 1, 2}, nil)
		require.NoError(t, err)
		w, err := New(FLAT, types.New(types.T_int64), fs)
		require.NoError(t, err)
		uf := w.GetUnionFunction()
		err = uf(v, nil)
		require.NoError(t, err)
		require.Equal(t, GetColumnValue[int64](v), GetColumnValue[int64](w))
		w, err = New(FLAT, types.New(types.T_int64), fs)
		require.NoError(t, err)
		uf = w.GetUnionFunction()
		err = uf(v, []uint32{1})
		require.NoError(t, err)
		require.Equal(t, []int64{1}, GetColumnValue[int64](w))
	}
	{ // test const vector
		v, err := New(CONSTANT, types.New(types.T_int64), fs)
		require.NoError(t, err)
		err = Append(v, int64(0), false)
		require.NoError(t, err)
		v.SetLength(3)
		w, err := New(FLAT, types.New(types.T_int64), fs)
		require.NoError(t, err)
		uf := w.GetUnionFunction()
		err = uf(v, nil)
		require.NoError(t, err)
		require.Equal(t, GetColumnValue[int64](v), GetColumnValue[int64](w)[:1])
		w, err = New(FLAT, types.New(types.T_int64), fs)
		require.NoError(t, err)
		uf = w.GetUnionFunction()
		err = uf(v, []uint32{1})
		require.NoError(t, err)
		require.Equal(t, []int64{0}, GetColumnValue[int64](w))
	}
	{
		v, err := New(FLAT, types.New(types.T_string), fs)
		require.NoError(t, err)
		err = AppendStringList(v, [][]byte{[]byte("x"), []byte("y")}, nil)
		require.NoError(t, err)
		w, err := New(FLAT, types.New(types.T_string), fs)
		require.NoError(t, err)
		uf := w.GetUnionFunction()
		err = uf(v, nil)
		require.NoError(t, err)
		{
			vs := GetColumnValue[types.String](v)
			ws := GetColumnValue[types.String](w)
			for i := range vs {
				vv, err := v.GetStringValue(vs[i])
				require.NoError(t, err)
				wv, err := w.GetStringValue(ws[i])
				require.NoError(t, err)
				require.Equal(t, vv, wv)
			}
		}
		w, err = New(FLAT, types.New(types.T_string), fs)
		require.NoError(t, err)
		uf = w.GetUnionFunction()
		err = uf(v, []uint32{1})
		require.NoError(t, err)
		{
			ws := GetColumnValue[types.String](w)
			wv, err := w.GetStringValue(ws[0])
			require.NoError(t, err)
			require.Equal(t, []byte("y"), wv)
		}
	}
}

func TestMarshalAndUnMarshal(t *testing.T) {
	fs := vfs.NewMemFS()
	v, err := New(FLAT, types.New(types.T_int64), fs)
	require.NoError(t, err)
	err = AppendList(v, []int64{0, 1, 2}, nil)
	require.NoError(t, err)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	w, err := New(FLAT, types.New(types.T_int64), fs)
	require.NoError(t, err)
	err = w.UnmarshalBinary(data)
	require.Equal(t, GetColumnValue[int64](v), GetColumnValue[int64](w))
	require.NoError(t, err)
}

func TestStrMarshalAndUnMarshal(t *testing.T) {
	fs := vfs.NewMemFS()
	v, err := New(FLAT, types.New(types.T_string), fs)
	require.NoError(t, err)
	err = AppendStringList(v, [][]byte{[]byte("x"), []byte("y")}, nil)
	require.NoError(t, err)
	v.nsp.Add(1)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	w, err := New(FLAT, types.New(types.T_string), fs)
	require.NoError(t, err)
	err = w.UnmarshalBinary(data)
	require.NoError(t, err)
	{ // check data
		vs := GetColumnValue[types.String](v)
		ws := GetColumnValue[types.String](w)
		for i := range vs {
			vv, err := v.GetStringValue(vs[i])
			require.NoError(t, err)
			wv, err := w.GetStringValue(ws[i])
			require.NoError(t, err)
			require.Equal(t, vv, wv)
		}
	}
}
