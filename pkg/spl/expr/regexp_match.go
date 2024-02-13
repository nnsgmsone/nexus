package expr

import (
	"regexp"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type regexpMatch struct {
	reg *regexp.Regexp
}

func newRegexpMatch() *regexpMatch {
	return &regexpMatch{}
}

func (r *regexpMatch) fn(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
	v0 := vector.GetColumnValue[types.String](args[0])
	v1 := vector.GetColumnValue[types.String](args[1])
	result.Reset()
	result.PreExtend(rows)
	switch {
	case args[0].IsConst() && args[1].IsConst():
		if args[0].IsNull(0) || args[1].IsNull(0) {
			for i := 0; i < rows; i++ {
				if err := vector.Append(result, false, true); err != nil {
					return err
				}
			}
			return nil
		}
		v0v, err := args[0].GetStringValue(v0[0])
		if err != nil {
			return err
		}
		if r.reg == nil {
			v1v, err := args[1].GetStringValue(v1[0])
			if err != nil {
				return err
			}
			if r.reg, err = regexp.Compile(encoding.Bytes2String(v1v)); err != nil {
				return err
			}
		}
		v := r.reg.Match(v0v)
		for i := 0; i < rows; i++ {
			if err := vector.Append(result, v, false); err != nil {
				return err
			}
		}
	case !args[0].IsConst() && args[1].IsConst():
		if args[1].IsNull(0) {
			for i := 0; i < rows; i++ {
				if err := vector.Append(result, false, true); err != nil {
					return err
				}
			}
			return nil
		}
		if r.reg == nil {
			v1v, err := args[1].GetStringValue(v1[0])
			if err != nil {
				return err
			}
			if r.reg, err = regexp.Compile(encoding.Bytes2String(v1v)); err != nil {
				return err
			}
		}
		for i := 0; i < rows; i++ {
			if args[0].IsNull(i) {
				if err := vector.Append(result, false, true); err != nil {
					return err
				}
				continue
			}
			v0v, err := args[0].GetStringValue(v0[i])
			if err != nil {
				return err
			}
			if err := vector.Append(result, r.reg.Match(v0v), false); err != nil {
				return err
			}
		}
	case args[0].IsConst() && !args[1].IsConst():
		if args[0].IsNull(0) {
			for i := 0; i < rows; i++ {
				if err := vector.Append(result, false, true); err != nil {
					return err
				}
			}
			return nil
		}
		v0v, err := args[0].GetStringValue(v0[0])
		if err != nil {
			return err
		}
		for i := 0; i < rows; i++ {
			if args[1].IsNull(i) {
				if err := vector.Append(result, false, true); err != nil {
					return err
				}
				continue
			}
			v1v, err := args[1].GetStringValue(v1[i])
			if err != nil {
				return err
			}
			r.reg, err = regexp.Compile(encoding.Bytes2String(v1v))
			if err != nil {
				return err
			}
			if err := vector.Append(result, r.reg.Match(v0v), false); err != nil {
				return err
			}
		}
	default:
		for i := 0; i < rows; i++ {
			if args[0].IsNull(i) || args[1].IsNull(i) {
				if err := vector.Append(result, false, true); err != nil {
					return err
				}
				continue
			}
			v0v, err := args[0].GetStringValue(v0[i])
			if err != nil {
				return err
			}
			v1v, err := args[1].GetStringValue(v1[i])
			if err != nil {
				return err
			}
			r.reg, err = regexp.Compile(encoding.Bytes2String(v1v))
			if err != nil {
				return err
			}
			if err := vector.Append(result, r.reg.Match(v0v), false); err != nil {
				return err
			}
		}
	}
	return nil
}
