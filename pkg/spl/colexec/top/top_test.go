package top

import (
	"testing"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/spl/plan"
	"github.com/nnsgmsone/nexus/pkg/util"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTop(t *testing.T) {
	{ // asc
		rows := [][]int64{
			{0, 0, 1, 1},
			{0, 0, 1, 1},
		}
		runTest(t, true, rows)
	}
	{ // desc
		rows := [][]int64{
			{9, 9, 8, 8},
			{1, 1, 0, 0},
		}
		runTest(t, false, rows)
	}
}

func runTest(t *testing.T, asc bool, rows [][]int64) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	attrs, ord := newTestTop(asc)
	op1 := New(attrs, ord, proc, 1)
	op2 := New(attrs, ord, proc, 1)
	mergeOp := New(attrs, ord, proc, 2)
	err := op1.Specialize()
	require.NoError(t, err)
	err = op2.Specialize()
	require.NoError(t, err)
	err = mergeOp.Specialize()
	require.NoError(t, err)
	bat := newTestBatch(t, fs)
	msg := util.NewDataMessage(bat)
	msgs, _, err := op1.Exec(msg)
	require.NoError(t, err)
	require.Equal(t, 0, len(msgs))
	msgs, _, err = op1.Exec(nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(msgs))
	msgs, _, err = mergeOp.Exec(msgs[0])
	require.NoError(t, err)
	require.Equal(t, 0, len(msgs))
	msgs, _, err = mergeOp.Exec(nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(msgs))
	msgs, _, err = op2.Exec(msg)
	require.NoError(t, err)
	require.Equal(t, 0, len(msgs))
	msgs, _, err = op2.Exec(nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(msgs))
	msgs, _, err = mergeOp.Exec(msgs[0])
	require.NoError(t, err)
	require.Equal(t, 0, len(msgs))
	msgs, _, err = mergeOp.Exec(nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(msgs))
	for i, row := range rows {
		require.Equal(t, row, vector.GetColumnValue[int64](msgs[0].GetBatch().GetVector(i)))
	}
}

func newTestTop(asc bool) ([]plan.ScopeAttribute, *plan.Order) {
	attrs := make([]plan.ScopeAttribute, 2)
	attrs[0] = plan.ScopeAttribute{
		ID:   0,
		Name: "a",
		Type: *types.New(types.T_int64),
	}
	attrs[1] = plan.ScopeAttribute{
		ID:   1,
		Name: "b",
		Type: *types.New(types.T_int64),
	}
	ord := &plan.Order{
		Limit: 4,
	}
	typ := plan.Ascending
	if !asc {
		typ = plan.Descending
	}
	ord.Orders = append(ord.Orders, plan.OrderBySpec{
		Type: typ,
		E:    expr.NewColExpr(0, 0, types.LongType),
	})
	return attrs, ord
}

func newTestBatch(t *testing.T, fs vfs.FS) *batch.Batch {
	vec0, err := vector.New(vector.FLAT, types.New(types.T_int64), fs)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		err = vector.Append(vec0, int64(i), false)
		require.NoError(t, err)
	}
	vec1, err := vector.New(vector.FLAT, types.New(types.T_int64), fs)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		err = vector.Append(vec1, int64(i)%2, false)
		require.NoError(t, err)
	}
	bat := batch.New(2, fs)
	bat.SetVector(0, vec0)
	bat.SetVector(1, vec1)
	bat.SetRows(10)
	return bat
}
