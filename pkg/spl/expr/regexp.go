package expr

import (
	"errors"
	"regexp"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type regexpExtract struct {
	reg *regexp.Regexp
}

type regexpExtractWithGroup struct {
	reg *regexp.Regexp
}

func newRegexpExtract() *regexpExtract {
	return &regexpExtract{}
}

func newRegexpExtractWithGroup() *regexpExtractWithGroup {
	return &regexpExtractWithGroup{}
}

func (r *regexpExtract) fn(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
	v0 := vector.GetColumnValue[types.String](args[0])
	v1 := vector.GetColumnValue[types.String](args[1])
	result.Reset()
	result.PreExtend(rows)
	switch {
	case args[0].IsConst() && args[1].IsConst():
		if args[0].IsNull(0) || args[1].IsNull(0) {
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
		if r.reg == nil {
			v1v, err := args[1].GetStringValue(v1[0])
			if err != nil {
				return err
			}
			r.reg, err = regexp.Compile(encoding.Bytes2String(v1v))
			if err != nil {
				return err
			}
		}
		if ret := r.reg.Find(v0v); len(ret) == 0 {
			for i := 0; i < rows; i++ {
				if err := vector.AppendString(result, nil, true); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < rows; i++ {
				if err := vector.AppendString(result, ret, false); err != nil {
					return err
				}
			}
		}
	case !args[0].IsConst() && args[1].IsConst():
		if args[1].IsNull(0) {
			for i := 0; i < rows; i++ {
				if err := vector.AppendString(result, nil, true); err != nil {
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
			r.reg, err = regexp.Compile(encoding.Bytes2String(v1v))
			if err != nil {
				return err
			}
		}
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
			if ret := r.reg.Find(v0v); len(ret) == 0 {
				if err := vector.AppendString(result, nil, true); err != nil {
					return err
				}
			} else {
				if err := vector.AppendString(result, ret, false); err != nil {
					return err
				}
			}
		}
	case args[0].IsConst() && !args[1].IsConst():
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
		for i := 0; i < rows; i++ {
			if args[1].IsNull(i) {
				if err := vector.AppendString(result, nil, true); err != nil {
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
			if ret := r.reg.Find(v0v); len(ret) == 0 {
				if err := vector.AppendString(result, nil, true); err != nil {
					return err
				}
			} else {
				if err := vector.AppendString(result, ret, false); err != nil {
					return err
				}
			}
		}
	default:
		for i := 0; i < rows; i++ {
			if args[0].IsNull(i) || args[1].IsNull(i) {
				if err := vector.AppendString(result, nil, true); err != nil {
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
			if ret := r.reg.Find(v0v); len(ret) == 0 {
				if err := vector.AppendString(result, nil, true); err != nil {
					return err
				}
			} else {
				if err := vector.AppendString(result, ret, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *regexpExtractWithGroup) fn(result *vector.Vector, args []*vector.Vector, proc *process.Process, rows int) error {
	if !args[2].IsConst() {
		return errors.New("regexp_extract: group must be constant")
	}
	if args[2].IsNull(0) {
		return errors.New("regexp_extract: group must be constant and not null")
	}
	grp := int(vector.GetColumnValue[int64](args[2])[0])
	if grp < 0 {
		return errors.New("regexp_extract: group must be positive")
	}
	v0 := vector.GetColumnValue[types.String](args[0])
	v1 := vector.GetColumnValue[types.String](args[1])
	result.Reset()
	result.PreExtend(rows)
	switch {
	case args[0].IsConst() && args[1].IsConst():
		if args[0].IsNull(0) || args[1].IsNull(0) {
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
		if r.reg == nil {
			v1v, err := args[1].GetStringValue(v1[0])
			if err != nil {
				return err
			}
			r.reg, err = regexp.Compile(encoding.Bytes2String(v1v))
			if err != nil {
				return err
			}
		}
		if ret := r.reg.FindSubmatch(v0v); len(ret) <= grp {
			for i := 0; i < rows; i++ {
				if err := vector.AppendString(result, nil, true); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < rows; i++ {
				if err := vector.AppendString(result, ret[grp], false); err != nil {
					return err
				}
			}
		}
	case !args[0].IsConst() && args[1].IsConst():
		if args[1].IsNull(0) {
			for i := 0; i < rows; i++ {
				if err := vector.AppendString(result, nil, true); err != nil {
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
			r.reg, err = regexp.Compile(encoding.Bytes2String(v1v))
			if err != nil {
				return err
			}
		}
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
			if ret := r.reg.FindSubmatch(v0v); len(ret) <= grp {
				if err := vector.AppendString(result, nil, true); err != nil {
					return err
				}
			} else {
				if err := vector.AppendString(result, ret[grp], false); err != nil {
					return err
				}
			}
		}
	case args[0].IsConst() && !args[1].IsConst():
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
		for i := 0; i < rows; i++ {
			if args[1].IsNull(i) {
				if err := vector.AppendString(result, nil, true); err != nil {
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
			if ret := r.reg.FindSubmatch(v0v); len(ret) <= grp {
				if err := vector.AppendString(result, nil, true); err != nil {
					return err
				}
			} else {
				if err := vector.AppendString(result, ret[grp], false); err != nil {
					return err
				}
			}
		}
	default:
		for i := 0; i < rows; i++ {
			if args[0].IsNull(i) || args[1].IsNull(i) {
				if err := vector.AppendString(result, nil, true); err != nil {
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
			if ret := r.reg.FindSubmatch(v0v); len(ret) <= grp {
				if err := vector.AppendString(result, nil, true); err != nil {
					return err
				}
			} else {
				if err := vector.AppendString(result, ret[grp], false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
