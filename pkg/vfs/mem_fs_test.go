package vfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemFS(t *testing.T) {
	fs := NewMemFS()
	// test create file and write
	fp, err := fs.Open("test")
	require.NoError(t, err)
	n, err := fp.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.NoError(t, fp.Close())
	// test read file
	fp, err = fs.Open("test")
	require.NoError(t, err)
	buf := make([]byte, 5)
	n, err = fp.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "hello", string(buf))
	// test ReadFile
	buf, err = fs.ReadFile("test")
	require.NoError(t, err)
	require.Equal(t, "hello", string(buf))
	// test Stat
	fi, err := fs.Stat("test")
	require.NoError(t, err)
	require.Equal(t, "test", fi.Name())
	require.Equal(t, int64(5), fi.Size())
	// test Remove
	require.NoError(t, fs.Remove("test"))
	_, err = fs.Stat("test")
	require.Error(t, err)
}
