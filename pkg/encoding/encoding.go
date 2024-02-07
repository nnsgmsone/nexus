package encoding

import (
	"unsafe"
)

func Encode[T any](v *T) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), unsafe.Sizeof(*v))
}

func Decode[T any](v []byte) T {
	return *(*T)(unsafe.Pointer(&v[0]))
}

func EncodeSlice[T any](vs []T) []byte {
	sz := int(unsafe.Sizeof(vs[0]))
	return unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*sz)[:len(vs)*sz]
}

func DecodeSlice[T any](vs []byte) []T {
	var v T

	sz := int(unsafe.Sizeof(v))
	return unsafe.Slice((*T)(unsafe.Pointer(&vs[0])), cap(vs)/sz)[:len(vs)/sz]
}

func Bytes2String(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

func String2Bytes(s string) []byte {
	if len(s) == 0 {
		return []byte{}
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
