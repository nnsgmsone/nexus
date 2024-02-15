package types

import (
	"fmt"
	"unsafe"
)

type T uint32

const (
	MaxInlineStringLength = 1 << 16
)

const (
	T_bool T = iota

	T_int64
	T_float64
	T_string
)

type Type struct {
	oid  T
	size int32 // e.g. int64.Size = 8
}

type Equaled interface {
	bool | int64 | float64
}

type String [2]uint64

func (typ *Type) Oid() T {
	return typ.oid
}

func (typ Type) IsType(oid T) bool {
	return typ.oid == oid
}

func (typ Type) IsFixedLen() bool {
	switch typ.oid {
	case T_bool, T_int64, T_float64:
		return true
	default:
		return false
	}
}

func (typ *Type) Equal(ityp *Type) bool {
	return typ.oid == ityp.oid && typ.size == ityp.size
}

func (typ *Type) String() string {
	return typ.oid.String()
}

func (typ *Type) Size() int {
	return int(typ.size)
}

func (t T) String() string {
	switch t {
	case T_bool:
		return "bool"
	case T_int64:
		return "long"
	case T_float64:
		return "double"
	case T_string:
		return "string"
	}
	return fmt.Sprintf("unexpected type: %d", t)
}

func (t T) Size() int {
	switch t {
	case T_bool:
		return 1
	case T_int64, T_float64:
		return 8
	case T_string:
		return 16
	}
	return -1
}

var TypeSize int

var BoolType Type
var LongType Type
var DoubleType Type
var StringType Type

func init() {
	var typ Type

	TypeSize = int(unsafe.Sizeof(typ))
	BoolType = *New(T_bool)
	LongType = *New(T_int64)
	DoubleType = *New(T_float64)
	StringType = *New(T_string)
}

func New(oid T) *Type {
	var typ Type

	typ.oid = oid
	typ.size = int32(oid.Size())
	return &typ
}
