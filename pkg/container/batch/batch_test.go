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
	"testing"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/stretchr/testify/require"
)

func TestBatch(t *testing.T) {
	fs := vfs.NewMemFS()
	bat := New(1, fs)
	{
		vec, err := vector.New(vector.FLAT, types.New(types.T_int64), fs)
		require.NoError(t, err)
		err = vector.Append(vec, int64(0), false)
		require.NoError(t, err)
		bat.SetVector(0, vec)
	}
	data, err := bat.MarshalBinary()
	require.NoError(t, err)
	nbat := New(1, fs)
	err = nbat.UnmarshalBinary(data)
	require.NoError(t, err)
	for i, vec := range bat.vecs {
		require.Equal(t, vec.String(), nbat.vecs[i].String())
	}
}

func TestBatchMarshalAndUnmarshal(t *testing.T) {
	fs := vfs.NewMemFS()
	bat := New(2, fs)
	{
		vec, err := vector.New(vector.FLAT, types.New(types.T_int64), fs)
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			err = vector.Append(vec, int64(i), false)
			require.NoError(t, err)
		}
		bat.SetVector(0, vec)
	}
	{
		vec, err := vector.New(vector.FLAT, types.New(types.T_int64), fs)
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			err = vector.Append(vec, int64(i), false)
			require.NoError(t, err)
		}
		bat.SetVector(1, vec)
	}
	data, err := bat.MarshalBinary()
	require.NoError(t, err)
	nbat := New(1, fs)
	err = nbat.UnmarshalBinary(data)
	require.NoError(t, err)
	for i, vec := range bat.vecs {
		require.Equal(t, vec.String(), nbat.vecs[i].String())
	}
}
