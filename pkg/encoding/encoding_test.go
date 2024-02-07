package encoding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	vs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		vs[i] = int64(i)
	}
	data := EncodeSlice(vs)
	require.Equal(t, vs, DecodeSlice[int64](data))

	var v int64

	data = Encode(&v)
	require.Equal(t, v, Decode[int64](data))
}

func TestEncodeDecodeSlice(t *testing.T) {
	vs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		vs[i] = int64(i)
	}
	data := EncodeSlice(vs)
	require.Equal(t, vs, DecodeSlice[int64](data))
}

func TestBytes2String(t *testing.T) {
	{
		b := []byte("hello")
		s := Bytes2String(b)
		require.Equal(t, "hello", s)
	}
	{ // nil test
		s := Bytes2String(nil)
		require.Equal(t, "", s)
	}
}

func TestString2Bytes(t *testing.T) {
	{
		s := "hello"
		b := String2Bytes(s)
		require.Equal(t, []byte("hello"), b)
	}
	{ // nil test
		b := String2Bytes("")
		require.Equal(t, []byte{}, b)
	}
}
