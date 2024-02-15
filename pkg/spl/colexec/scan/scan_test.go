package scan

import (
	"os"
	"testing"

	"github.com/nnsgmsone/nexus/pkg/testutil"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestScan(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	e := testutil.NewTestEngine()
	r, err := e.NewReader(proc)
	require.NoError(t, err)
	lua, err := os.ReadFile("../../../../lua/csv.lua")
	require.NoError(t, err)
	scanOp := New(r, string(lua), []int{0, 2}, proc)
	require.NoError(t, scanOp.Specialize())
	for {
		msgs, state, err := scanOp.Exec(nil)
		require.NoError(t, err)
		if state == pipeline.END {
			break
		}
		require.Equal(t, 1, len(msgs))
		require.Equal(t, pipeline.EVAL, state)
		bat := msgs[0].GetBatch()
		require.Equal(t, 2, bat.VectorCount())
		require.Equal(t, 8192, bat.Rows())
	}
}
