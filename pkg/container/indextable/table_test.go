package indextable

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	TestRows  = 4 << 10
	TestBatch = 512
)

func TestTable(t *testing.T) {
	tbl := New[uint64](0)
	hs := make([]uint64, TestBatch)
	keys := make([]uint64, TestBatch)
	values := make([]uint64, TestBatch)
	testKeys := make([]uint64, TestRows)
	for i := range testKeys {
		testKeys[i] = uint64(i)
	}
	cnt := 1
	for i := 0; i < TestRows; i += TestBatch {
		copy(keys, testKeys[i:i+TestBatch])
		tbl.Insert(hs, keys, values)
		for j := 0; j < TestBatch; j++ {
			require.Equal(t, uint64(cnt), values[j])
			cnt++
		}
	}
	for i := 0; i < TestRows; i += TestBatch {
		copy(keys, testKeys[i:i+TestBatch])
		tbl.Lookup(hs, keys, values)
		for j := 0; j < TestBatch; j++ {
			require.Equal(t, uint64(i+j)+1, values[j])
		}
	}
	require.Equal(t, TestRows+1, cnt)
}

func TestBytesTable(t *testing.T) {
	tbl := NewBytesTable(0)
	hs := make([]uint64, TestBatch)
	keys := make([][]byte, TestBatch)
	values := make([]uint64, TestBatch)
	testKeys := make([][]byte, TestRows)
	for i := range testKeys {
		testKeys[i] = []byte(fmt.Sprintf("%v", i))
	}
	cnt := 1
	for i := 0; i < TestRows; i += TestBatch {
		copy(keys, testKeys[i:i+TestBatch])
		tbl.Insert(hs, keys, values)
		for j := 0; j < TestBatch; j++ {
			require.Equal(t, uint64(cnt), values[j])
			cnt++
		}
	}
	for i := 0; i < TestRows; i += TestBatch {
		copy(keys, testKeys[i:i+TestBatch])
		tbl.Lookup(hs, keys, values)
		for j := 0; j < TestBatch; j++ {
			require.Equal(t, uint64(i+j)+1, values[j])
		}
	}
	require.Equal(t, TestRows+1, cnt)
}
