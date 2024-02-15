package agg

import (
	"fmt"
	"testing"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/testutil"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	TestRows = 10
)

func TestCount(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("count", []types.Type{types.LongType}, types.LongType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(1)
	vec := testutil.NewVector(TestRows, types.LongType, false)
	err = a.Fill([]uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("count", []types.Type{types.LongType}, types.LongType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(1)
	err = b.Fill([]uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("count", []types.Type{types.LongType}, types.LongType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0}, []uint32{0})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[int64](rvec)
	require.Equal(t, 20, int(vs[0]))
}

func TestCountWithGroup(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("count", []types.Type{types.LongType}, types.LongType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(5)
	vec := testutil.NewVector(TestRows, types.LongType, false)
	err = a.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("count", []types.Type{types.LongType}, types.LongType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(5)
	err = b.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("count", []types.Type{types.LongType}, types.LongType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[int64](rvec)
	require.Equal(t, []int64{4, 4, 4, 4, 4}, vs)
}

func TestSum(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("sum", []types.Type{types.DoubleType}, types.DoubleType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(1)
	vec := testutil.NewVector(TestRows, types.DoubleType, false)
	err = a.Fill([]uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("sum", []types.Type{types.DoubleType}, types.DoubleType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(1)
	err = b.Fill([]uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("sum", []types.Type{types.DoubleType}, types.DoubleType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0}, []uint32{0})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[float64](rvec)
	require.Equal(t, float64(90), vs[0])
}

func TestSumWithGroup(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("sum", []types.Type{types.DoubleType}, types.DoubleType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(5)
	vec := testutil.NewVector(TestRows, types.DoubleType, false)
	err = a.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("sum", []types.Type{types.DoubleType}, types.DoubleType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(5)
	err = b.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("sum", []types.Type{types.DoubleType}, types.DoubleType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[float64](rvec)
	require.Equal(t, []float64{10, 14, 18, 22, 26}, vs)
}

func TestAvg(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("avg", []types.Type{types.LongType}, types.DoubleType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(1)
	vec := testutil.NewVector(TestRows, types.LongType, false)
	err = a.Fill([]uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("avg", []types.Type{types.LongType}, types.DoubleType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(1)
	err = b.Fill([]uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("avg", []types.Type{types.LongType}, types.DoubleType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0}, []uint32{0})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[float64](rvec)
	require.Equal(t, float64(4.5), vs[0])

}

func TestAvgWithGroup(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("avg", []types.Type{types.LongType}, types.DoubleType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(5)
	vec := testutil.NewVector(TestRows, types.LongType, false)
	err = a.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("avg", []types.Type{types.LongType}, types.DoubleType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(5)
	err = b.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("avg", []types.Type{types.LongType}, types.DoubleType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[float64](rvec)
	require.Equal(t, []float64{2.5, 3.5, 4.5, 5.5, 6.5}, vs)

}

func TestMin(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("min", []types.Type{types.LongType}, types.LongType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(1)
	vec := testutil.NewVector(TestRows, types.LongType, false)
	err = a.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("min", []types.Type{types.LongType}, types.LongType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(1)
	err = b.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("min", []types.Type{types.LongType}, types.LongType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0}, []uint32{0})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[int64](rvec)
	require.Equal(t, int64(0), vs[0])
}

func TestBoolMin(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("min", []types.Type{types.BoolType}, types.BoolType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(1)
	vec := testutil.NewVector(TestRows, types.BoolType, false)
	err = a.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("min", []types.Type{types.BoolType}, types.BoolType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(1)
	err = b.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("min", []types.Type{types.BoolType}, types.BoolType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0}, []uint32{0})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[bool](rvec)
	require.Equal(t, false, vs[0])
}

func TestStrMin(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("min", []types.Type{types.StringType}, types.StringType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(1)
	vec := testutil.NewVector(TestRows, types.StringType, false)
	err = a.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("min", []types.Type{types.StringType}, types.StringType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(1)
	err = b.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("min", []types.Type{types.StringType}, types.StringType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Specialize(proc)
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0}, []uint32{0})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[types.String](rvec)
	str, err := rvec.GetStringValue(vs[0])
	require.NoError(t, err)
	require.Equal(t, []byte("0"), str)
}

func TestMinWithGroup(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("min", []types.Type{types.LongType}, types.LongType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(5)
	vec := testutil.NewVector(TestRows, types.LongType, false)
	err = a.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("min", []types.Type{types.LongType}, types.LongType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(5)
	err = b.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("min", []types.Type{types.LongType}, types.LongType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[int64](rvec)
	require.Equal(t, []int64{0, 1, 2, 3, 4}, vs)
}

func TestBoolMinWithGroup(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("min", []types.Type{types.BoolType}, types.BoolType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(5)
	vec := testutil.NewVector(TestRows, types.BoolType, false)
	err = a.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("min", []types.Type{types.BoolType}, types.BoolType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(5)
	err = b.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("min", []types.Type{types.BoolType}, types.BoolType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[bool](rvec)
	require.Equal(t, []bool{false, false, false, false, false}, vs)
}

func TestStrMinWithGroup(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("min", []types.Type{types.StringType}, types.StringType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(5)
	vec := testutil.NewVector(TestRows, types.StringType, false)
	err = a.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("min", []types.Type{types.StringType}, types.StringType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(5)
	err = b.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("min", []types.Type{types.StringType}, types.StringType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Specialize(proc)
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[types.String](rvec)
	for i, v := range vs {
		str, err := rvec.GetStringValue(v)
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("%v", i)), str)
	}
}

func TestMax(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("max", []types.Type{types.LongType}, types.LongType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(1)
	vec := testutil.NewVector(TestRows, types.LongType, false)
	err = a.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("max", []types.Type{types.LongType}, types.LongType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(1)
	err = b.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("max", []types.Type{types.LongType}, types.LongType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0}, []uint32{0})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[int64](rvec)
	require.Equal(t, int64(9), vs[0])
}

func TestBoolMax(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("max", []types.Type{types.BoolType}, types.BoolType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(1)
	vec := testutil.NewVector(TestRows, types.BoolType, false)
	err = a.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("max", []types.Type{types.BoolType}, types.BoolType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(1)
	err = b.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("max", []types.Type{types.BoolType}, types.BoolType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0}, []uint32{0})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[bool](rvec)
	require.Equal(t, true, vs[0])
}

func TestStrMax(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("max", []types.Type{types.StringType}, types.StringType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(1)
	vec := testutil.NewVector(TestRows, types.StringType, false)
	err = a.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("max", []types.Type{types.StringType}, types.StringType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(1)
	err = b.BulkFill(0, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("max", []types.Type{types.StringType}, types.StringType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Specialize(proc)
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0}, []uint32{0})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[types.String](rvec)
	str, err := rvec.GetStringValue(vs[0])
	require.NoError(t, err)
	require.Equal(t, []byte("9"), str)
}

func TestMaxWithGroup(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("max", []types.Type{types.LongType}, types.LongType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(5)
	vec := testutil.NewVector(TestRows, types.LongType, false)
	err = a.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("max", []types.Type{types.LongType}, types.LongType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(5)
	err = b.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("max", []types.Type{types.LongType}, types.LongType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[int64](rvec)
	require.Equal(t, []int64{5, 6, 7, 8, 9}, vs)
}

func TestBoolMaxWithGroup(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("max", []types.Type{types.BoolType}, types.BoolType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(5)
	vec := testutil.NewVector(TestRows, types.BoolType, false)
	err = a.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("max", []types.Type{types.BoolType}, types.BoolType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(5)
	err = b.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("max", []types.Type{types.BoolType}, types.BoolType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[bool](rvec)
	require.Equal(t, []bool{true, true, true, true, true}, vs)
}

func TestStrMaxWithGroup(t *testing.T) {
	fs := vfs.NewMemFS()
	proc := process.New(fs)
	a := New("max", []types.Type{types.StringType}, types.StringType)
	err := a.Specialize(proc)
	require.NoError(t, err)
	a.Grows(5)
	vec := testutil.NewVector(TestRows, types.StringType, false)
	err = a.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	b := New("max", []types.Type{types.StringType}, types.StringType)
	err = b.Specialize(proc)
	require.NoError(t, err)
	b.Grows(5)
	err = b.Fill([]uint32{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, []*vector.Vector{vec})
	require.NoError(t, err)
	c := New("max", []types.Type{types.StringType}, types.StringType)
	bvec, err := b.Save()
	require.NoError(t, err)
	err = c.Specialize(proc)
	require.NoError(t, err)
	err = c.Load(bvec)
	require.NoError(t, err)
	err = a.Merge(c, []uint32{0, 1, 2, 3, 4}, []uint32{0, 1, 2, 3, 4})
	require.NoError(t, err)
	rvec, err := a.Eval()
	require.NoError(t, err)
	vs := vector.GetColumnValue[types.String](rvec)
	for i, v := range vs {
		str, err := rvec.GetStringValue(v)
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("%v", i+5)), str)
	}
}
