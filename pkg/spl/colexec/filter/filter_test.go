package filter

import (
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

func TestFilterOp(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	colExpr := expr.NewColExpr(0, 0, types.LongType)
	constExpr, err := expr.NewConstExpr(false, constant.MakeInt64(10))
	require.NoError(t, err)
	expr, err := expr.NewFuncExpr("<", []expr.Expr{colExpr, constExpr})
	require.NoError(t, err)
	op := New(expr, proc)
	err = op.Specialize()
	require.NoError(t, err)
	bat := newTestBatch(t, fs)
	msg := util.NewDataMessage(bat)
	msgs, _, err := op.Exec(msg)
	require.NoError(t, err)
	vec := msgs[0].GetBatch().GetVector(0)
	require.Equal(t, 10, vec.Length())
	vs := vector.GetColumnValue[int64](vec)
	for i, v := range vs {
		require.Equal(t, int64(i), v)
	}

}

func TestConstTrueFilterOp(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	expr, err := expr.NewConstExpr(false, constant.MakeBool(true))
	require.NoError(t, err)
	op := New(expr, proc)
	err = op.Specialize()
	require.NoError(t, err)
	bat := newTestBatch(t, fs)
	msg := util.NewDataMessage(bat)
	msgs, _, err := op.Exec(msg)
	require.NoError(t, err)
	vec := msgs[0].GetBatch().GetVector(0)
	require.Equal(t, 8192, vec.Length())
	vs := vector.GetColumnValue[int64](vec)
	for i, v := range vs {
		require.Equal(t, int64(i), v)
	}
}

func TestConstFalseFilterOp(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	expr, err := expr.NewConstExpr(false, constant.MakeBool(false))
	require.NoError(t, err)
	op := New(expr, proc)
	err = op.Specialize()
	require.NoError(t, err)
	bat := newTestBatch(t, fs)
	msg := util.NewDataMessage(bat)
	msgs, _, err := op.Exec(msg)
	require.NoError(t, err)
	require.Equal(t, 0, len(msgs))
}

func newTestBatch(t *testing.T, fs vfs.FS) *batch.Batch {
	vec, err := vector.New(vector.FLAT, types.New(types.T_int64), fs)
	require.NoError(t, err)
	for i := 0; i < 8192; i++ {
		err = vector.Append(vec, int64(i), false)
		require.NoError(t, err)
	}
	bat := batch.New(1, fs)
	bat.SetVector(0, vec)
	bat.SetRows(8192)
	return bat
}
