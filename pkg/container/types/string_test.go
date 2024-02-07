package types

import (
	"testing"

	"github.com/google/uuid"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	var err error
	var area []byte

	str := new(String)
	fs := vfs.NewMemFS()
	area, err = str.SetString([]byte("hello"), area, fs)
	require.NoError(t, err)
	v, _, _ := str.GetString(area)
	require.Equal(t, "hello", string(v))
	area, err = str.SetString(make([]byte, 1<<18), area, fs)
	require.NoError(t, err)
	v, id, _ := str.GetString(area)
	require.Equal(t, 0, len(v))
	rv, err := fs.ReadFile(id.String())
	require.NoError(t, err)
	require.Equal(t, make([]byte, 1<<18), rv)
	id, err = uuid.NewV7()
	require.NoError(t, err)
	err = fs.WriteFile(id.String(), make([]byte, 1<<20))
	require.NoError(t, err)
	area = str.SetStringUUID(id, area, 1<<20)
	v, id, _ = str.GetString(area)
	require.Equal(t, 0, len(v))
	rv, err = fs.ReadFile(id.String())
	require.NoError(t, err)
	require.Equal(t, make([]byte, 1<<20), rv)
}
