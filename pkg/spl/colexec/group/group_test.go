package group

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

func TestSummarize(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	grp := newTestGroup(t)
	grp.GroupBy = nil // no group by
	op1 := New(grp, false, proc, 1)
	op2 := New(grp, false, proc, 1)
	mergeOp := New(grp, true, proc, 2)
	bat := newTestBatch(t, fs)
	err := op1.Specialize()
	require.NoError(t, err)
	err = op2.Specialize()
	require.NoError(t, err)
	err = mergeOp.Specialize()
	require.NoError(t, err)
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
	vs := vector.GetColumnValue[int64](msgs[0].GetBatch().GetVector(0))
	require.Equal(t, int64(20), vs[0])
}

func TestGroup(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	grp := newTestGroup(t)
	op1 := New(grp, false, proc, 1)
	op2 := New(grp, false, proc, 1)
	mergeOp := New(grp, true, proc, 2)
	bat := newTestBatch(t, fs)
	err := op1.Specialize()
	require.NoError(t, err)
	err = op2.Specialize()
	require.NoError(t, err)
	err = mergeOp.Specialize()
	require.NoError(t, err)
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
	msgs, _, err = mergeOp.Exec(nil)
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
	rbat := msgs[0].GetBatch()
	col0 := vector.GetColumnValue[int64](rbat.GetVector(0))
	require.Equal(t, []int64{0, 1}, col0)
	col1 := vector.GetColumnValue[int64](rbat.GetVector(1))
	require.Equal(t, []int64{10, 10}, col1)
}

func newTestGroup(t *testing.T) *plan.Group {
	var err error

	grp := &plan.Group{}
	agg := plan.Aggregate{
		Name:  "a",
		FName: "count",
	}
	agg.Es = append(agg.Es, expr.NewColExpr(0, 0, types.LongType))
	agg.Agg, err = expr.NewFuncExpr("count", agg.Es)
	require.NoError(t, err)
	grp.AggList = append(grp.AggList, agg)
	grp.GroupBy = append(grp.GroupBy, expr.NewColExpr(0, 1, types.LongType))
	return grp
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
