package expr

import (
	"bytes"
	"errors"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

func replace(result *vector.Vector, args []*vector.Vector, p *process.Process, rows int) error {
	if !args[1].IsConst() || !args[2].IsConst() {
		return errors.New("replace: (old, new) must be constant")
	}
	if args[1].IsNull(0) || args[2].IsNull(0) {
		return errors.New("replace: (old, new) must be constant and not null")
	}
	v0 := vector.GetColumnValue[types.String](args[0])
	v1 := vector.GetColumnValue[types.String](args[1])
	v2 := vector.GetColumnValue[types.String](args[2])
	old, err := args[1].GetStringValue(v1[0])
	if err != nil {
		return err
	}
	new, err := args[2].GetStringValue(v2[0])
	if err != nil {
		return err
	}
	result.Reset()
	result.PreExtend(rows)
	switch {
	case args[0].IsConst():
		if args[0].IsNull(0) {
			for i := 0; i < rows; i++ {
				if err := vector.AppendString(result, nil, true); err != nil {
					return err
				}
			}
			return nil
		}
		v0v, err := args[0].GetStringValue(v0[0])
		if err != nil {
			return err
		}
		v := bytes.ReplaceAll(v0v, old, new)
		for i := 0; i < rows; i++ {
			if err := vector.AppendString(result, v, false); err != nil {
				return err
			}
		}
	default:
		for i := 0; i < rows; i++ {
			if args[0].IsNull(i) {
				if err := vector.AppendString(result, nil, true); err != nil {
					return err
				}
				continue
			}
			v0v, err := args[0].GetStringValue(v0[i])
			if err != nil {
				return err
			}
			v := bytes.ReplaceAll(v0v, old, new)
			if err := vector.AppendString(result, v, false); err != nil {
				return err
			}
		}
	}
	return nil
}
