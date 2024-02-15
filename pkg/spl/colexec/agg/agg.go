package agg

import (
	"strings"

	"github.com/nnsgmsone/nexus/pkg/container/types"
)

func New(name string, args []types.Type, typ types.Type) Agg {
	switch strings.ToLower(name) {
	case "min":
		return newMin(args, typ)
	case "max":
		return newMax(args, typ)
	case "sum":
		return newSum(args, typ)
	case "avg":
		return newAvg(args, typ)
	case "count":
		return newCount(args, typ)
	default:
		panic("unknown agg")
	}
}

func newMin(args []types.Type, typ types.Type) Agg {
	switch args[0].Oid() {
	case types.T_int64:
		return &min[int64]{typ: typ}
	case types.T_float64:
		return &min[float64]{typ: typ}
	case types.T_bool:
		return &boolMin{typ: typ}
	case types.T_string:
		return &strMin{typ: typ}
	default:
		panic("invalid type")
	}
}

func newMax(args []types.Type, typ types.Type) Agg {
	switch args[0].Oid() {
	case types.T_int64:
		return &max[int64]{typ: typ}
	case types.T_float64:
		return &max[float64]{typ: typ}
	case types.T_bool:
		return &boolMax{typ: typ}
	case types.T_string:
		return &strMax{typ: typ}
	default:
		panic("invalid type")
	}
}

func newSum(args []types.Type, typ types.Type) Agg {
	switch args[0].Oid() {
	case types.T_int64:
		return &sum[int64]{typ: typ}
	case types.T_float64:
		return &sum[float64]{typ: typ}
	default:
		panic("invalid type")
	}
}

func newAvg(args []types.Type, typ types.Type) Agg {
	switch args[0].Oid() {
	case types.T_int64:
		return &avg[int64]{typ: typ}
	case types.T_float64:
		return &avg[float64]{typ: typ}
	default:
		panic("invalid type")
	}
}

func newCount(args []types.Type, typ types.Type) Agg {
	return &count{typ: typ}
}
