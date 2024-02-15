package compile

import (
	"fmt"
	"os"
	"testing"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/testutil"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/engine"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCompile(t *testing.T) {
	emit := func(bat *batch.Batch) error {
		return nil
	}
	lua, err := os.ReadFile("../../../lua/csv.lua")
	require.NoError(t, err)
	e := testutil.NewTestEngine()
	proc := process.New(vfs.NewMemFS())
	runSql(t, fmt.Sprintf("| extract lua = `%s` a=0 | where true", string(lua)), emit, e, proc)
	runSql(t, fmt.Sprintf("| extract lua = `%s` a=0 | where false", string(lua)), emit, e, proc)
	runSql(t, fmt.Sprintf("| extract lua = `%s` a=0 | limit 1", string(lua)), emit, e, proc)
	runSql(t, fmt.Sprintf("| extract lua = `%s` a=0, b=1 | limit 1 | stats count(b) as d by a | where a = \"1\" | sort 10 by d ", string(lua)), emit, e, proc)
	runSql(t, fmt.Sprintf("| extract lua = `%s` a=0 | limit 1 | eval d =a | sort 10 by d", string(lua)), emit, e, proc)
	runSql(t, fmt.Sprintf("| extract lua = `%s` a=0 | limit 1 | stats count() as d by a | where d = 1 | sort 10 by d", string(lua)), emit, e, proc)
	runSql(t, fmt.Sprintf("| extract lua = `%s` a=0 | limit 1 | stats count()", string(lua)), emit, e, proc)
	runSql(t, fmt.Sprintf("| extract lua = `%s` a=0, c=2 | limit 1 | stats count(a) by c | where c = \"1\" and 'count(a)' = 0", string(lua)), emit, e, proc)
}

func runSql(t *testing.T, sql string, emit func(*batch.Batch) error,
	e engine.Engine, proc *process.Process) {
	c, err := New(sql, e, proc, emit)
	require.NoError(t, err)
	err = c.Compile()
	require.NoError(t, err)
	err = c.Run()
	require.NoError(t, err)
}
