package noah

import (
	"bytes"
	"testing"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/testutil"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestNoah(t *testing.T) {
	fs := vfs.NewFS()
	err := fs.Mkdir("noah-data")
	require.NoError(t, err)
	err = fs.ChDir("noah-data")
	require.NoError(t, err)
	e, err := New(fs)
	require.NoError(t, err)
	// test Write
	vec := testutil.NewStringVector(10, types.StringType, false)
	err = e.Write(vec)
	require.NoError(t, err)
	err = e.Write(vec)
	require.NoError(t, err)
	proc := process.New(fs)
	err = e.Write(vec)
	require.NoError(t, err)
	r, err := e.NewReader(proc)
	require.NoError(t, err)
	rvec := testutil.NewStringVector(10, types.StringType, false)
	r.Specialize()
	buf := new(bytes.Buffer)
	for {
		err = r.Read(rvec, buf)
		require.NoError(t, err)
		if rvec.Length() == 0 {
			break
		}
		require.Equal(t, 10, rvec.Length())
		require.Equal(t, vec, rvec)
	}
	// test Truncate
	err = e.Clean()
	require.NoError(t, err)
	r, err = e.NewReader(proc)
	require.NoError(t, err)
	rows := 0
	for {
		err = r.Read(rvec, buf)
		require.NoError(t, err)
		if rvec.Length() == 0 {
			break
		}
		rows += rvec.Length()
	}
	require.Equal(t, 0, rows)
	err = fs.ChDir("..")
	require.NoError(t, err)
	err = fs.Remove("noah-data")
	require.NoError(t, err)
}
