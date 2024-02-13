package expr

import (
	"bytes"
	"strconv"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

func matchFunctionOverload(args []types.Type, o overload) bool {
	if len(args) != len(o.args) {
		return false
	}
	for i := range args {
		if !(&args[i]).Equal(&o.args[i]) {
			return false
		}
	}
	return true
}

var functionRegistry = []funcRegistration{
	// function 'sum'
	Sum: {
		fid:           Sum,
		class:         AGGREGATE,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.LongType,
				args: []types.Type{types.LongType},
			},
			{
				id:   1,
				rtyp: types.DoubleType,
				args: []types.Type{types.DoubleType},
			},
		},
	},
	// function 'avg'
	Avg: {
		fid:           Avg,
		class:         AGGREGATE,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.DoubleType,
				args: []types.Type{types.LongType},
			},
			{
				id:   1,
				rtyp: types.DoubleType,
				args: []types.Type{types.DoubleType},
			},
		},
	},
	// function 'Min'
	Min: {
		fid:           Min,
		class:         AGGREGATE,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType},
			},
			{
				id:   1,
				rtyp: types.LongType,
				args: []types.Type{types.LongType},
			},
			{
				id:   2,
				rtyp: types.DoubleType,
				args: []types.Type{types.DoubleType},
			},
			{
				id:   3,
				rtyp: types.StringType,
				args: []types.Type{types.StringType},
			},
		},
	},
	// function 'Max'
	Max: {
		fid:           Max,
		class:         AGGREGATE,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType},
			},
			{
				id:   1,
				rtyp: types.LongType,
				args: []types.Type{types.LongType},
			},
			{
				id:   2,
				rtyp: types.DoubleType,
				args: []types.Type{types.DoubleType},
			},
			{
				id:   3,
				rtyp: types.StringType,
				args: []types.Type{types.StringType},
			},
		},
	},
	// function 'Count'
	Count: {
		fid:           Count,
		class:         AGGREGATE,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.LongType,
				args: []types.Type{types.BoolType},
			},
			{
				id:   1,
				rtyp: types.LongType,
				args: []types.Type{types.LongType},
			},
			{
				id:   2,
				rtyp: types.LongType,
				args: []types.Type{types.DoubleType},
			},
			{
				id:   3,
				rtyp: types.LongType,
				args: []types.Type{types.StringType},
			},
		},
	},

	Minus: {
		fid:           Minus,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.LongType,
				args: []types.Type{types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return unaryFunc(result, args, proc, rows, func(v int64) (int64, error) { return -v, nil })
					}
				},
			},
			{
				id:   1,
				rtyp: types.DoubleType,
				args: []types.Type{types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return unaryFunc(result, args, proc, rows, func(v float64) (float64, error) { return -v, nil })
					}
				},
			},
			{
				id:   2,
				rtyp: types.LongType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 int64) (int64, error) { return v1 - v2, nil })
					}
				},
			},
			{
				id:   3,
				rtyp: types.DoubleType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 float64) (float64, error) { return v1 - v2, nil })
					}
				},
			},
		},
	},

	Plus: {
		fid:           Plus,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.LongType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 int64) (int64, error) { return v1 + v2, nil })
					}
				},
			},
			{
				id:   1,
				rtyp: types.DoubleType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 float64) (float64, error) { return v1 + v2, nil })
					}
				},
			},
		},
	},

	Mult: {
		fid:           Mult,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.LongType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 int64) (int64, error) { return v1 * v2, nil })
					}
				},
			},
			{
				id:   1,
				rtyp: types.DoubleType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 float64) (float64, error) { return v1 * v2, nil })
					}
				},
			},
		},
	},
	Div: {
		fid:           Div,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.LongType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 int64) (int64, error) {
							if v2 == 0 {
								return 0, ErrDivByZero
							}
							return v1 / v2, nil
						})
					}
				},
			},
			{
				id:   1,
				rtyp: types.DoubleType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 float64) (float64, error) {
							if v2 == 0 {
								return 0, ErrDivByZero
							}
							return v1 / v2, nil
						})
					}
				},
			},
		},
	},

	Mod: {
		fid:           Mod,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.LongType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 int64) (int64, error) {
							if v2 == 0 {
								return 0, ErrModByZero
							}
							return v1 % v2, nil
						})
					}
				},
			},
			{
				id:   1,
				rtyp: types.DoubleType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 float64) (float64, error) {
							if v2 == 0 {
								return 0, ErrModByZero
							}
							return float64(int64(v1) % int64(v2)), nil
						})
					}
				},
			},
		},
	},

	// function 'isnull'
	ISNULL: {
		fid:           ISNULL,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return isNullFunc(result, args, proc, rows, true)
					}
				},
			},
			{
				id:   1,
				rtyp: types.BoolType,
				args: []types.Type{types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return isNullFunc(result, args, proc, rows, true)
					}
				},
			},
			{
				id:   2,
				rtyp: types.BoolType,
				args: []types.Type{types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return isNullFunc(result, args, proc, rows, true)
					}
				},
			},
			{
				id:   3,
				rtyp: types.BoolType,
				args: []types.Type{types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return isNullFunc(result, args, proc, rows, true)
					}
				},
			},
		},
	},

	// function 'isnotnull'
	ISNOTNULL: {
		fid:           ISNOTNULL,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return isNullFunc(result, args, proc, rows, false)
					}
				},
			},
			{
				id:   1,
				rtyp: types.BoolType,
				args: []types.Type{types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return isNullFunc(result, args, proc, rows, false)
					}
				},
			},
			{
				id:   2,
				rtyp: types.BoolType,
				args: []types.Type{types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return isNullFunc(result, args, proc, rows, false)
					}
				},
			},
			{
				id:   3,
				rtyp: types.BoolType,
				args: []types.Type{types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return isNullFunc(result, args, proc, rows, false)
					}
				},
			},
		},
	},

	// function 'not'
	Not: {
		fid:           Not,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return unaryFunc(result, args, proc, rows, func(v bool) (bool, error) { return !v, nil })
					}
				},
			},
		},
	},
	// function 'and'
	And: {
		fid:           And,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 bool) (bool, error) { return v1 && v2, nil })
					}
				},
			},
		},
	},
	// function 'or'
	Or: {
		fid:           Or,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 bool) (bool, error) { return v1 || v2, nil })
					}
				},
			},
		},
	},
	// function '='
	EQ: {
		fid:           EQ,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 bool) (bool, error) { return v1 == v2, nil })
					}
				},
			},
			{
				id:   1,
				rtyp: types.BoolType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 int64) (bool, error) { return v1 == v2, nil })
					}
				},
			},
			{
				id:   2,
				rtyp: types.BoolType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1, v2 float64) (bool, error) { return v1 == v2, nil })
					}
				},
			},
			{
				id:   3,
				rtyp: types.BoolType,
				args: []types.Type{types.StringType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryStringFunc(result, args, proc, rows, func(v1 []byte, v2 []byte) (bool, error) { return bytes.Equal(v1, v2), nil })
					}
				},
			},
		},
	},

	LT: {
		fid:           LT,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b bool) (bool, error) { return !a && b, nil })
					}
				},
			},
			{
				id:   1,
				rtyp: types.BoolType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b int64) (bool, error) { return a < b, nil })
					}
				},
			},
			{
				id:   2,
				rtyp: types.BoolType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b float64) (bool, error) { return a < b, nil })
					}
				},
			},
			{
				id:   3,
				rtyp: types.BoolType,
				args: []types.Type{types.StringType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryStringFunc(result, args, proc, rows, func(a, b []byte) (bool, error) { return bytes.Compare(a, b) < 0, nil })
					}
				},
			},
		},
	},

	LE: {
		fid:           LE,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b bool) (bool, error) { return !a || b, nil })
					}
				},
			},
			{
				id:   1,
				rtyp: types.BoolType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b int64) (bool, error) { return a <= b, nil })
					}
				},
			},
			{
				id:   2,
				rtyp: types.BoolType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b float64) (bool, error) { return a <= b, nil })
					}
				},
			},
			{
				id:   3,
				rtyp: types.BoolType,
				args: []types.Type{types.StringType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryStringFunc(result, args, proc, rows, func(a, b []byte) (bool, error) { return bytes.Compare(a, b) <= 0, nil })
					}
				},
			},
		},
	},

	GT: {
		fid:           GT,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b bool) (bool, error) { return a && !b, nil })
					}
				},
			},
			{
				id:   1,
				rtyp: types.BoolType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b int64) (bool, error) { return a > b, nil })
					}
				},
			},
			{
				id:   2,
				rtyp: types.BoolType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b float64) (bool, error) { return a > b, nil })
					}
				},
			},
			{
				id:   3,
				rtyp: types.BoolType,
				args: []types.Type{types.StringType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryStringFunc(result, args, proc, rows, func(a, b []byte) (bool, error) { return bytes.Compare(a, b) > 0, nil })
					}
				},
			},
		},
	},

	GE: {
		fid:           GE,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) (err error) {
						return binaryFunc(result, args, proc, rows, func(a, b bool) (bool, error) { return a || !b, nil })
					}
				},
			},
			{
				id:   1,
				rtyp: types.BoolType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) (err error) {
						return binaryFunc(result, args, proc, rows, func(a, b int64) (bool, error) { return a >= b, nil })
					}
				},
			},
			{
				id:   2,
				rtyp: types.BoolType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) (err error) {
						return binaryFunc(result, args, proc, rows, func(a, b float64) (bool, error) { return a >= b, nil })
					}
				},
			},
			{
				id:   3,
				rtyp: types.BoolType,
				args: []types.Type{types.StringType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) (err error) {
						return binaryStringFunc(result, args, proc, rows, func(a, b []byte) (bool, error) { return bytes.Compare(a, b) >= 0, nil })
					}
				},
			},
		},
	},

	NE: {
		fid:           NE,
		class:         NORMAL,
		typeConvertFn: defaultConvertFunc,
		overloads: []overload{
			{
				id:   0, // bool
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b bool) (bool, error) { return a != b, nil })
					}
				},
			},
			{
				id:   1, // long
				rtyp: types.BoolType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b int64) (bool, error) { return a != b, nil })
					}
				},
			},
			{
				id:   2, // double
				rtyp: types.BoolType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a, b float64) (bool, error) { return a != b, nil })
					}
				},
			},
			{
				id:   3, // string
				rtyp: types.BoolType,
				args: []types.Type{types.StringType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryStringFunc(result, args, proc, rows, func(a, b []byte) (bool, error) { return !bytes.Equal(a, b), nil })
					}
				},
			},
		},
	},

	// function 'cast'
	Typecast: {
		fid:   Typecast,
		class: NORMAL,
		typeConvertFn: func(args []types.Type) []types.Type {
			return args
		},
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.BoolType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a bool, _ bool) (bool, error) { return a, nil })
					}
				},
			},
			{
				id:   1,
				rtyp: types.BoolType,
				args: []types.Type{types.LongType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1 int64, _ bool) (bool, error) { return v1 != 0, nil })
					}
				},
			},
			{
				id:   2,
				rtyp: types.BoolType,
				args: []types.Type{types.DoubleType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1 float64, _ bool) (bool, error) { return v1 != 0, nil })
					}
				},
			},
			{
				id:   3,
				rtyp: types.BoolType,
				args: []types.Type{types.StringType, types.BoolType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryStringWithOtherFunc(result, args, proc, rows, func(v1 []byte, v2 bool) (bool, bool, error) {

							v := bytes.ToLower(v1)
							switch {
							case bytes.Equal(v, constTrue):
								return true, false, nil
							case bytes.Equal(v, constFalse):
								return false, false, nil
							default:
								return false, true, nil
							}
						})
					}
				},
			},
			{
				id:   4,
				rtyp: types.LongType,
				args: []types.Type{types.LongType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows,
							func(a int64, _ int64) (int64, error) { return a, nil })
					}
				},
			},
			{
				id:   5,
				rtyp: types.LongType,
				args: []types.Type{types.BoolType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1 bool, _ int64) (int64, error) {
							if v1 {
								return 1, nil
							}
							return 0, nil
						})
					}
				},
			},
			{
				id:   6,
				rtyp: types.LongType,
				args: []types.Type{types.DoubleType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(v1 float64, _ int64) (int64, error) { return int64(v1), nil })
					}
				},
			},
			{
				id:   7,
				rtyp: types.LongType,
				args: []types.Type{types.StringType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryStringWithOtherFunc(result, args, proc, rows, func(v1 []byte, _ int64) (int64, bool, error) {
							r, err := strconv.Atoi(encoding.Bytes2String(v1))
							if err != nil {
								return 0, true, err
							}
							return int64(r), false, nil
						})
					}
				},
			},
			{
				id:   8,
				rtyp: types.DoubleType,
				args: []types.Type{types.DoubleType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args, proc, rows, func(a float64, _ float64) (float64, error) { return a, nil })
					}
				},
			},
			{
				id:   9,
				rtyp: types.DoubleType,
				args: []types.Type{types.BoolType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result, args,
							proc, rows, func(v1 bool, _ float64) (float64, error) {
								if v1 {
									return 1, nil
								}
								return 0, nil
							})
					}
				},
			},
			{
				id:   10,
				rtyp: types.DoubleType,
				args: []types.Type{types.LongType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result,
							args, proc, rows, func(v1 int64, _ float64) (float64, error) { return float64(v1), nil })
					}
				},
			},
			{
				id:   11,
				rtyp: types.DoubleType,
				args: []types.Type{types.StringType, types.DoubleType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryStringWithOtherFunc(result, args, proc, rows, func(v1 []byte, _ float64) (float64, bool, error) {
							r, err := strconv.ParseFloat(encoding.Bytes2String(v1), 64)
							if err != nil {
								return 0, true, err
							}
							return r, false, nil
						})
					}
				},
			},
			{
				id:   12,
				rtyp: types.StringType,
				args: []types.Type{types.StringType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryStringFunc(result, args, proc, rows, func(a, _ []byte) ([]byte, error) { return a, nil })
					}
				},
			},
			{
				id:   13,
				rtyp: types.StringType,
				args: []types.Type{types.BoolType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result,
							args, proc, rows, func(v1 bool, _ []byte) ([]byte, error) { return []byte(strconv.FormatBool(v1)), nil })
					}
				},
			},
			{
				id:   14,
				rtyp: types.StringType,
				args: []types.Type{types.LongType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result,
							args, proc, rows, func(v1 int64, _ []byte) ([]byte, error) { return []byte(strconv.FormatInt(v1, 10)), nil })
					}
				},
			},
			{
				id:   15,
				rtyp: types.StringType,
				args: []types.Type{types.DoubleType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
						return binaryFunc(result,
							args, proc, rows, func(v1 float64, _ []byte) ([]byte, error) { return []byte(strconv.FormatFloat(v1, 'f', -1, 64)), nil })
					}
				},
			},
		},
	},

	RegexpMatch: {
		fid:   RegexpMatch,
		class: NORMAL,
		typeConvertFn: func(args []types.Type) []types.Type {
			return args
		},
		overloads: []overload{
			{
				id:   0,
				rtyp: types.BoolType,
				args: []types.Type{types.StringType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return newRegexpMatch().fn
				},
			},
		},
	},

	Replace: {
		fid:   Replace,
		class: NORMAL,
		typeConvertFn: func(args []types.Type) []types.Type {
			return args
		},
		overloads: []overload{
			{
				id:   0,
				rtyp: types.StringType,
				args: []types.Type{types.StringType, types.StringType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return replace
				},
			},
		},
	},

	RegexpExtract: {
		fid:   RegexpExtract,
		class: NORMAL,
		typeConvertFn: func(args []types.Type) []types.Type {
			return args
		},
		overloads: []overload{
			{
				id:   0,
				rtyp: types.StringType,
				args: []types.Type{types.StringType, types.StringType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return newRegexpExtract().fn
				},
			},
			{
				id:   1,
				rtyp: types.StringType,
				args: []types.Type{types.StringType, types.StringType, types.LongType},
				fn: func() func(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
					return newRegexpExtractWithGroup().fn
				},
			},
		},
	},
}

func isNullFunc(result *vector.Vector, args []*vector.Vector,
	proc *process.Process, rows int, isNull bool) error {
	result.Reset()
	result.PreExtend(rows)
	switch {
	case args[0].IsConst():
		v := args[0].IsNull(0)
		if !isNull {
			v = !v
		}
		for i := 0; i < rows; i++ {
			if err := vector.Append(result, v, false); err != nil {
				return err
			}
		}
	default:
		for i := 0; i < rows; i++ {
			v := args[0].IsNull(i)
			if !isNull {
				v = !v
			}
			if err := vector.Append(result, v, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func unaryFunc[T1 any, Tr any](result *vector.Vector, args []*vector.Vector,
	proc *process.Process, rows int, fn func(T1) (Tr, error)) error {
	vs := vector.GetColumnValue[T1](args[0])
	result.Reset()
	result.PreExtend(rows)
	switch {
	case args[0].IsConst():
		r, err := fn(vs[0])
		if err != nil {
			return err
		}
		isNull := args[0].IsNull(0)
		for i := 0; i < rows; i++ {
			if err = vector.Append(result, r, isNull); err != nil {
				return err
			}
		}
	default:
		for i := 0; i < rows; i++ {
			r, err := fn(vs[i])
			if err != nil {
				return err
			}
			if err = vector.Append(result, r, args[0].IsNull(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func binaryFunc[T1 any, T2 any, Tr any](result *vector.Vector,
	args []*vector.Vector, proc *process.Process, rows int,
	fn func(T1, T2) (Tr, error)) error {
	v1 := vector.GetColumnValue[T1](args[0])
	v2 := vector.GetColumnValue[T2](args[1])
	result.Reset()
	result.PreExtend(rows)
	switch {
	case args[0].IsConst() && args[1].IsConst():
		r, err := fn(v1[0], v2[0])
		if err != nil {
			return err
		}
		isNull := args[0].IsNull(0) || args[1].IsNull(0)
		for i := 0; i < rows; i++ {
			if err = vector.Append(result, r, isNull); err != nil {
				return err
			}
		}
	case !args[0].IsConst() && args[1].IsConst():
		if args[1].IsNull(0) {
			var r Tr
			for i := 0; i < rows; i++ {
				if err := vector.Append(result, r, true); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < rows; i++ {
				r, err := fn(v1[i], v2[0])
				if err != nil {
					return err
				}
				if err := vector.Append(result, r, args[0].IsNull(i)); err != nil {
					return err
				}
			}
		}
	case args[0].IsConst() && !args[1].IsConst():
		if args[0].IsNull(0) {
			var r Tr
			for i := 0; i < rows; i++ {
				if err := vector.Append(result, r, true); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < rows; i++ {
				r, err := fn(v1[i], v2[0])
				if err != nil {
					return err
				}
				if err = vector.Append(result, r, args[1].IsNull(i)); err != nil {
					return err
				}
			}
		}
	default:
		for i := 0; i < rows; i++ {
			r, err := fn(v1[i], v2[i])
			if err != nil {
				return err
			}
			if err = vector.Append(result, r,
				args[0].IsNull(i) || args[1].IsNull(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func binaryStringFunc[Tr any](result *vector.Vector,
	args []*vector.Vector, proc *process.Process, rows int, fn func([]byte, []byte) (Tr, error)) error {
	v1 := vector.GetColumnValue[types.String](args[0])
	v2 := vector.GetColumnValue[types.String](args[1])
	result.Reset()
	result.PreExtend(rows)
	switch {
	case args[0].IsConst() && args[1].IsConst():
		isNull := args[0].IsNull(0) || args[1].IsNull(0)
		v1v, err := args[0].GetStringValue(v1[0])
		if err != nil {
			return err
		}
		v2v, err := args[1].GetStringValue(v2[0])
		if err != nil {
			return err
		}
		r, err := fn(v1v, v2v)
		if err != nil {
			return err
		}
		for i := 0; i < rows; i++ {
			if err := vector.Append(result, r, isNull); err != nil {
				return err
			}
		}
	case !args[0].IsConst() && args[1].IsConst():
		if args[1].IsNull(0) {
			var r Tr
			for i := 0; i < rows; i++ {
				if err := vector.Append(result, r, true); err != nil {
					return err
				}
			}
		} else {
			cv, err := args[1].GetStringValue(v2[0]) // get const string value
			if err != nil {
				return err
			}
			for i := 0; i < rows; i++ {
				v1v, err := args[0].GetStringValue(v1[i])
				if err != nil {
					return err
				}
				r, err := fn(v1v, cv)
				if err != nil {
					return err
				}
				if err = vector.Append(result, r, args[0].IsNull(i)); err != nil {
					return err
				}
			}
		}
	case args[0].IsConst() && !args[1].IsConst():
		if args[0].IsNull(0) {
			var r Tr
			for i := 0; i < rows; i++ {
				if err := vector.Append(result, r, true); err != nil {
					return err
				}
			}
		} else {
			cv, err := args[0].GetStringValue(v1[0]) // get const string value
			if err != nil {
				return err
			}
			for i := 0; i < rows; i++ {
				v2v, err := args[1].GetStringValue(v2[i])
				if err != nil {
					return err
				}
				r, err := fn(cv, v2v)
				if err != nil {
					return err
				}
				if err = vector.Append(result, r, args[1].IsNull(i)); err != nil {
					return err
				}
			}
		}
	default:
		for i := 0; i < rows; i++ {
			v1v, err := args[0].GetStringValue(v1[i])
			if err != nil {
				return err
			}
			v2v, err := args[1].GetStringValue(v2[i])
			if err != nil {
				return err
			}
			r, err := fn(v1v, v2v)
			if err != nil {
				return err
			}
			if err := vector.Append(result, r,
				args[0].IsNull(i) || args[1].IsNull(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func binaryStringWithOtherFunc[T1 any, Tr any](result *vector.Vector,
	args []*vector.Vector, proc *process.Process, rows int, fn func([]byte, T1) (Tr, bool, error)) error {
	v1 := vector.GetColumnValue[types.String](args[0])
	v2 := vector.GetColumnValue[T1](args[1])
	result.Reset()
	result.PreExtend(rows)
	switch {
	case args[0].IsConst() && args[1].IsConst():
		v1v, err := args[0].GetStringValue(v1[0])
		if err != nil {
			return err
		}
		r, isNull, err := fn(v1v, v2[0])
		if err != nil {
			return err
		}
		isNull = isNull || args[0].IsNull(0) || args[1].IsNull(0)
		for i := 0; i < rows; i++ {
			if err = vector.Append(result, r, isNull); err != nil {
				return err
			}
		}
	case !args[0].IsConst() && args[1].IsConst():
		if args[1].IsNull(0) {
			var r Tr
			for i := 0; i < rows; i++ {
				if err := vector.Append(result, r, true); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < rows; i++ {
				v1v, err := args[0].GetStringValue(v1[i])
				if err != nil {
					return err
				}
				r, isNull, err := fn(v1v, v2[0])
				if err != nil {
					return err
				}
				if err := vector.Append(result, r, isNull || args[0].IsNull(i)); err != nil {
					return err
				}
			}
		}
	case args[0].IsConst() && !args[1].IsConst():
		if args[0].IsNull(0) {
			var r Tr
			for i := 0; i < rows; i++ {
				if err := vector.Append(result, r, true); err != nil {
					return err
				}
			}
		} else {
			cv, err := args[0].GetStringValue(v1[0]) // get const string value
			if err != nil {
				return err
			}
			for i := 0; i < rows; i++ {
				r, isNull, err := fn(cv, v2[i])
				if err != nil {
					return err
				}
				if err := vector.Append(result, r, isNull || args[1].IsNull(i)); err != nil {
					return err
				}
			}
		}
	default:
		for i := 0; i < rows; i++ {
			v1v, err := args[0].GetStringValue(v1[i])
			if err != nil {
				return err
			}
			r, isNull, err := fn(v1v, v2[i])
			if err != nil {
				return err
			}
			if err := vector.Append(result, r,
				isNull || args[0].IsNull(i) || args[1].IsNull(i)); err != nil {
				return err
			}
		}
	}
	return nil
}
