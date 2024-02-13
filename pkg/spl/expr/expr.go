package expr

import (
	"fmt"
	"go/constant"
	"strings"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

func NewConstExpr(isNull bool, val constant.Value) (*ConstExpr, error) {
	var e ConstExpr

	e.val = val
	e.isNull = isNull
	switch val.Kind() {
	case constant.Int:
		e.typ = *types.New(types.T_int64)
	case constant.Bool:
		e.typ = *types.New(types.T_bool)
	case constant.Float:
		e.typ = *types.New(types.T_float64)
	case constant.String:
		e.typ = *types.New(types.T_string)
	default:
		return nil, fmt.Errorf("unexpected constant value '%v'", val)
	}
	return &e, nil
}

func NewFuncExpr(name string, args []Expr) (*FuncExpr, error) {
	id, ok := NameOp[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("function '%s' not yet implemented", name)
	}
	fr := functionRegistry[id]
	typs := make([]types.Type, len(args))
	for i, arg := range args {
		typs[i] = arg.ResultType()
	}
	rtyps := fr.typeConvertFn(typs)
	for _, o := range fr.overloads {
		if matchFunctionOverload(rtyps, o) {
			for i := range args {
				if !(&typs[i]).Equal(&rtyps[i]) {
					var err error
					var constExpr *ConstExpr

					switch rtyps[i].Oid() {
					case types.T_bool:
						constExpr, err = NewConstExpr(false, constant.MakeBool(true))
					case types.T_int64:
						constExpr, err = NewConstExpr(false, constant.MakeInt64(0))
					case types.T_float64:
						constExpr, err = NewConstExpr(false, constant.MakeFloat64(0))
					case types.T_string:
						constExpr, err = NewConstExpr(false, constant.MakeString(""))
					}
					if err != nil {
						return nil, err
					}
					if args[i], err = NewFuncExpr("cast", []Expr{args[i], constExpr}); err != nil {
						return nil, err
					}
				}
			}
			return &FuncExpr{
				args:  args,
				newfn: o.fn,
				typ:   o.rtyp,
				fid:   uint64(id)<<32 | uint64(o.id),
			}, nil
		}
	}
	return nil, fmt.Errorf("function '%s' not yet implemented for arguments '%v'", name, args)
}

func NewColExpr(relPos, colPos uint32, typ types.Type) *ColExpr {
	return &ColExpr{
		typ:    typ,
		colPos: colPos,
		relPos: relPos,
	}
}

func (e *ConstExpr) ResultType() types.Type {
	return e.typ
}

func (e *FuncExpr) ResultType() types.Type {
	return e.typ
}

func (e *ColExpr) ResultType() types.Type {
	return e.typ
}

func (e *ConstExpr) Specialize(proc *process.Process) error {
	switch e.val.Kind() {
	case constant.Int:
		vec, err := vector.New(vector.CONSTANT, types.New(types.T_int64), proc.FS())
		if err != nil {
			return err
		}
		v, ok := constant.Int64Val(e.val)
		if !ok {
			return fmt.Errorf("type of constant value '%v' is not int64", e.val)
		}
		e.vec = vec
		return vector.Append(vec, v, e.isNull)
	case constant.Bool:
		vec, err := vector.New(vector.CONSTANT, types.New(types.T_bool), proc.FS())
		if err != nil {
			return err
		}
		e.vec = vec
		return vector.Append(vec, constant.BoolVal(e.val), e.isNull)
	case constant.Float:
		vec, err := vector.New(vector.CONSTANT, types.New(types.T_float64), proc.FS())
		if err != nil {
			return err
		}
		v, ok := constant.Float64Val(e.val)
		if !ok {
			return fmt.Errorf("type of constant value '%v' is not float64", e.val)
		}
		e.vec = vec
		return vector.Append(vec, v, e.isNull)
	case constant.String:
		vec, err := vector.New(vector.CONSTANT, types.New(types.T_string), proc.FS())
		if err != nil {
			return err
		}
		e.vec = vec
		return vector.AppendString(vec, encoding.String2Bytes(constant.StringVal(e.val)), e.isNull)
	default:
		return fmt.Errorf("unexpected constant value '%v'", e.val)
	}
}

func (e *FuncExpr) Specialize(proc *process.Process) error {
	for i := range e.args {
		if err := e.args[i].Specialize(proc); err != nil {
			return err
		}
	}
	vec, err := vector.New(vector.FLAT, &e.typ, proc.FS())
	if err != nil {
		return err
	}
	e.vec = vec
	e.fn = e.newfn()
	e.vecs = make([]*vector.Vector, len(e.args))
	return nil
}

func (e *ColExpr) Specialize(proc *process.Process) error {
	return nil
}

func (e *ConstExpr) Eval(_ []*batch.Batch, proc *process.Process) (*vector.Vector, error) {
	return e.vec, nil
}

func (e *FuncExpr) Eval(bats []*batch.Batch, proc *process.Process) (*vector.Vector, error) {
	for i, arg := range e.args {
		vec, err := arg.Eval(bats, proc)
		if err != nil {
			return nil, err
		}
		e.vecs[i] = vec
	}
	return e.vec, e.fn(e.vec, e.vecs, proc, bats[0].Rows())
}

func (e *ColExpr) Eval(bats []*batch.Batch, proc *process.Process) (*vector.Vector, error) {
	return bats[e.relPos].GetVector(int(e.colPos)), nil
}

func (e *ConstExpr) IterateAllColExpr(fn func(uint32, uint32) (uint32, uint32)) {
}

func (e *FuncExpr) IterateAllColExpr(fn func(uint32, uint32) (uint32, uint32)) {
	for i := range e.args {
		e.args[i].IterateAllColExpr(fn)
	}
}

func (e *ColExpr) IterateAllColExpr(fn func(uint32, uint32) (uint32, uint32)) {
	e.relPos, e.colPos = fn(e.relPos, e.colPos)
}

func (e *ColExpr) SplitBy(_ uint32) []Expr {
	return []Expr{e}
}

func (e *ConstExpr) SplitBy(_ uint32) []Expr {
	return []Expr{e}
}

func (e *FuncExpr) SplitBy(op uint32) []Expr {
	if uint32((e.fid >> 32)) == op {
		var es []Expr

		for i := range e.args {
			es = append(es, e.args[i].SplitBy(op)...)
		}
		return es
	}
	return []Expr{e}
}

func (e *ColExpr) Dup() Expr {
	return &ColExpr{
		typ:    e.typ,
		colPos: e.colPos,
		relPos: e.relPos,
	}
}

func (e *ConstExpr) Dup() Expr {
	return &ConstExpr{
		val:    e.val,
		typ:    e.typ,
		isNull: e.isNull,
	}
}

func (e *FuncExpr) Dup() Expr {
	args := make([]Expr, len(e.args))
	for i := range e.args {
		args[i] = e.args[i].Dup()
	}
	return &FuncExpr{
		args:  args,
		typ:   e.typ,
		fid:   e.fid,
		newfn: e.newfn,
	}
}
