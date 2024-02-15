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
	"bytes"
	"fmt"

	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/defines"
	"github.com/nnsgmsone/nexus/pkg/vm/engine"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type testEngine struct {
}

type testReader struct {
	cnt int
}

func NewTestEngine() engine.Engine {
	return &testEngine{}
}

func (e *testEngine) Clean() error {
	return nil
}

func (e *testEngine) Write(_ *vector.Vector) error {
	panic("not implement")
}

func (d *testEngine) NewReader(_ *process.Process) (engine.Reader, error) {
	return &testReader{}, nil
}

func (r *testReader) Specialize() error {
	r.cnt = 2 // test count
	return nil
}

func (r *testReader) Read(vec *vector.Vector, buf *bytes.Buffer) error {
	vec.Reset()
	if r.cnt == 0 {
		return nil
	}
	for i := 0; i < defines.DefaultRows; i++ {
		if err := vector.AppendString(vec, []byte(fmt.Sprintf("%d,%d,%d\n", i, i+1, i+2)), false); err != nil {
			panic(err)
		}
	}
	r.cnt--
	return nil
}
