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

package plan

import (
	"fmt"
	"testing"

	"github.com/nnsgmsone/nexus/pkg/testutil"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/stretchr/testify/require"
)

func TestBuild(t *testing.T) {
	fs := vfs.NewFS()
	e := testutil.NewTestEngine()
	plan, err := New("| extract lua = \"x\" a = 0, b = 1 |  limit 1 | stats count(b) as d by a | where a = true | sort 10 by d", fs, e).Build()
	require.NoError(t, err)
	fmt.Printf("%s\n", plan.Root)
	plan, err = New("| extract lua = \"x\" a = 2 |  limit 1 | eval d = a | sort 10 by d", fs, e).Build()
	require.NoError(t, err)
	fmt.Printf("%s\n", plan.Root)
	plan, err = New("| extract lua = \"x\" a = 2 |  limit 1 | eval d = a | where d = true | sort 10 by d", fs, e).Build()
	require.NoError(t, err)
	fmt.Printf("%s\n", plan.Root)
	plan, err = New("| extract lua = \"x\" a = 1 |  limit 1 | stats count() as d by a | where d = 1 | sort 10 by d", fs, e).Build()
	require.NoError(t, err)
	fmt.Printf("%s\n", plan.Root)
	plan, err = New("| extract lua = \"xx\" x=0 |  limit 1 | stats count()", fs, e).Build()
	require.NoError(t, err)
	fmt.Printf("%s\n", plan.Root)
	plan, err = New("| extract lua = \"x\" a = 1, c = 3 |  stats count(a) by c | where c = 1 and 'count(a)' = 0", fs, e).Build()
	require.NoError(t, err)
	fmt.Printf("%s\n", plan.Root)
	plan, err = New("| import \"1.csv\", \"2.csv\"", fs, e).Build()
	require.NoError(t, err)
	fmt.Printf("%s\n", plan.Root)
}
