package bitmap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitmap(t *testing.T) {
	bm, err := New(100)
	require.NoError(t, err)
	require.Equal(t, "[]", bm.String())
	// test IsEmpty
	require.True(t, bm.IsEmpty())
	// test Add
	bm.Add(0)
	require.Equal(t, "[0]", bm.String())
	bm.Add(1)
	require.Equal(t, "[0,1]", bm.String())
	bm.Add(2)
	require.Equal(t, "[0,1,2]", bm.String())
	bm.Add(3)
	require.Equal(t, "[0,1,2,3]", bm.String())
	// test Contains
	require.True(t, bm.Contains(0))
	require.True(t, bm.Contains(1))
	require.True(t, bm.Contains(2))
	require.True(t, bm.Contains(3))
	// test count
	require.Equal(t, 4, bm.Count())
	// test Marshal and Unmarhal
	data, err := bm.Marshal()
	require.NoError(t, err)
	tmp, err := New(0)
	err = tmp.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, "[0,1,2,3]", tmp.String())
	// test Reset
	tmp.Reset()
	require.True(t, tmp.IsEmpty())
	// test Remove
	bm.Remove(1)
	require.Equal(t, "[0,2,3]", bm.String())
	require.False(t, bm.Contains(1))
	// testShrink
	err = bm.Shrink([]uint32{0, 1, 2})
	require.NoError(t, err)
	require.Equal(t, "[0,2]", bm.String())
}
