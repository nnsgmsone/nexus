package expr

import "github.com/nnsgmsone/nexus/pkg/container/types"

var convertRule [255][255][]types.Type

// default implicit type conversion table
func init() {
	convertRule[types.T_int64][types.T_float64] = []types.Type{
		types.DoubleType, types.DoubleType,
	}
	convertRule[types.T_float64][types.T_int64] = []types.Type{
		types.DoubleType, types.DoubleType,
	}

	convertRule[types.T_bool][types.T_bool] = []types.Type{
		types.BoolType, types.BoolType,
	}
	convertRule[types.T_int64][types.T_int64] = []types.Type{
		types.LongType, types.LongType,
	}
	convertRule[types.T_float64][types.T_float64] = []types.Type{
		types.DoubleType, types.DoubleType,
	}
	convertRule[types.T_string][types.T_string] = []types.Type{
		types.StringType, types.StringType,
	}
	// bool default implicit rule
	convertRule[types.T_bool][types.T_string] = []types.Type{
		types.BoolType, types.BoolType,
	}
	// long default implicit rule
	convertRule[types.T_int64][types.T_string] = []types.Type{
		types.LongType, types.LongType,
	}
	// double default implicit rule
	convertRule[types.T_float64][types.T_string] = []types.Type{
		types.DoubleType, types.DoubleType,
	}
	// string default implicit rule
	convertRule[types.T_string][types.T_bool] = []types.Type{
		types.BoolType, types.BoolType,
	}
	convertRule[types.T_string][types.T_int64] = []types.Type{
		types.LongType, types.LongType,
	}
	convertRule[types.T_string][types.T_float64] = []types.Type{
		types.DoubleType, types.DoubleType,
	}
}

func defaultConvertFunc(args []types.Type) []types.Type {
	if len(args) == 2 {
		return convertRule[args[0].Oid()][args[1].Oid()]
	}
	return args
}
