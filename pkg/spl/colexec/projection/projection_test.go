package projection

import (
	"fmt"
	"go/constant"
	"testing"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/util"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestProjection(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	colExpr := expr.NewColExpr(0, 0, types.StringType)
	constExpr, err := expr.NewConstExpr(false, constant.MakeString("xx[0-1]yy"))
	require.NoError(t, err)
	idxExpr, err := expr.NewConstExpr(false, constant.MakeInt64(0))
	require.NoError(t, err)
	e, err := expr.NewFuncExpr("regexp_extract", []expr.Expr{colExpr, constExpr, idxExpr})
	require.NoError(t, err)
	op := New([]expr.Expr{e}, proc)
	err = op.Specialize()
	require.NoError(t, err)
	bat := newTestBatch(t, fs)
	msg := util.NewDataMessage(bat)
	msgs, _, err := op.Exec(msg)
	require.NoError(t, err)
	vec := msgs[0].GetBatch().GetVector(0)
	require.Equal(t, 8192, vec.Length())
	cnt := 0
	strs := vector.GetColumnValue[types.String](vec)
	for i := range strs {
		str, err := vec.GetStringValue(strs[i])
		require.NoError(t, err)
		if len(str) > 0 {
			cnt++
		}
	}
	require.Equal(t, 1640, cnt)
}

func newTestBatch(t *testing.T, fs vfs.FS) *batch.Batch {
	vec, err := vector.New(vector.FLAT, types.New(types.T_string), fs)
	require.NoError(t, err)
	for i := 0; i < 8192; i++ {
		err = vector.AppendString(vec, []byte(fmt.Sprintf("[xx%dyy]", i%10)), false)
		require.NoError(t, err)
	}
	bat := batch.New(1, fs)
	bat.SetVector(0, vec)
	bat.SetRows(8192)
	return bat
}
