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
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/vfs"
)

func New(n int, fs vfs.FS) *Batch {
	var bat Batch

	bat.fs = fs
	bat.vecs = make([]*vector.Vector, n)
	return &bat
}

func (bat *Batch) SchemaEqual(ibat *Batch) bool {
	for i := range bat.vecs {
		if !bat.vecs[i].TypeEqual(ibat.vecs[i]) {
			return false
		}
	}
	return true
}

func (bat *Batch) Reset() {
	for i := range bat.vecs {
		bat.vecs[i].Reset()
	}
	bat.rows = 0
}

func (bat *Batch) Dup() *Batch {
	rbat := New(len(bat.vecs), bat.fs)
	for i, vec := range bat.vecs {
		rbat.vecs[i] = vec.Dup()
	}
	rbat.rows = bat.rows
	return rbat
}

func (bat *Batch) Append(ibat *Batch) {
	for i := range bat.vecs {
		bat.vecs[i].GetUnionFunction()(ibat.vecs[i], nil)
	}
	bat.rows = ibat.rows
}

func (bat *Batch) Shrink(sels []uint32) {
	mp := make(map[*vector.Vector]uint8)
	for _, vec := range bat.vecs {
		if _, ok := mp[vec]; ok {
			continue
		}
		mp[vec]++
		sf := vector.GetShrinkFunction(vec.GetType())
		sf(vec, sels)
	}
	bat.rows = int32(len(sels))
}
