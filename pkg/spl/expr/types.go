package expr

import (
	"errors"
	"fmt"
	"go/constant"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

const (
	NORMAL = iota
	AGGREGATE
)

var (
	constTrue  = []byte("true")
	constFalse = []byte("false")
)

const (
	Not = iota

	Or
	And
	Plus
	Minus
	Mult
	Div
	Mod
	Replace
	Typecast
	RegexpMatch
	RegexpExtract

	EQ
	LT
	LE
	GT
	GE
	NE

	IN

	ISNULL
	ISNOTNULL

	Sum
	Avg
	Min
	Max
	Count
)

var OpName = map[int]string{
	Not: "not",

	Or:    "or",
	And:   "and",
	Plus:  "+",
	Minus: "-",
	Mult:  "*",
	Div:   "/",
	Mod:   "%",

	Replace:       "replace",
	Typecast:      "cast",
	RegexpMatch:   "regexp_match",
	RegexpExtract: "regexp_extract",

	EQ: "=",
	LT: "<",
	LE: "<=",
	GT: ">",
	GE: ">=",
	NE: "<>",

	IN: "in",

	ISNULL:    "isnull",
	ISNOTNULL: "isnotnull",

	Sum:   "sum",
	Avg:   "avg",
	Min:   "min",
	Max:   "max",
	Count: "count",
}

var NameOp = map[string]int{
	"not": Not,

	"or":  Or,
	"and": And,
	"+":   Plus,
	"-":   Minus,
	"*":   Mult,
	"/":   Div,
	"%":   Mod,

	"cast": Typecast,

	"replace":        Replace,
	"regexp_match":   RegexpMatch,
	"regexp_extract": RegexpExtract,

	"=":  EQ,
	"<":  LT,
	"<=": LE,
	">":  GT,
	">=": GE,
	"<>": NE,

	"in": IN,

	"isnull":    ISNULL,
	"isnotnull": ISNOTNULL,

	"sum":   Sum,
	"avg":   Avg,
	"min":   Min,
	"max":   Max,
	"count": Count,
}

var ErrDivByZero = errors.New("division by zero")
var ErrModByZero = errors.New("division by zero")

type Expr interface {
	Dup() Expr
	String() string
	ResultType() types.Type
	Specialize(*process.Process) error
	Eval([]*batch.Batch, *process.Process) (*vector.Vector, error)
	// iterate through all expr
	IterateAllColExpr(func(uint32, uint32) (uint32, uint32))

	// slice the expression according to a specific function
	SplitBy(uint32) []Expr
}

type ConstExpr struct {
	isNull bool
	typ    types.Type
	val    constant.Value
	vec    *vector.Vector
}

type FuncExpr struct {
	fid   uint64 // fid = fid | overload id
	args  []Expr
	typ   types.Type
	vec   *vector.Vector
	vecs  []*vector.Vector
	fn    func(*vector.Vector, []*vector.Vector, *process.Process, int) error
	newfn func() func(*vector.Vector, []*vector.Vector, *process.Process, int) error
}

type ColExpr struct {
	colPos uint32
	relPos uint32
	typ    types.Type
}

type overload struct {
	id   uint32
	rtyp types.Type
	args []types.Type
	fn   func() func(*vector.Vector, []*vector.Vector, *process.Process, int) error
}

// funcRegistration is all the information needed for a function to be registered,
// including the function's registration id, function's type and all overloaded
type funcRegistration struct {
	fid           uint32
	class         uint32 // function class like agg or normal
	overloads     []overload
	typeConvertFn func([]types.Type) []types.Type
}

func (e *ConstExpr) ConstValue() constant.Value {
	return e.val
}

func (e *ColExpr) String() string {
	return fmt.Sprintf("%v:%v", e.relPos, e.colPos)
}

func (e *ConstExpr) String() string {
	return fmt.Sprintf("%v", e.val)
}

func (e *FuncExpr) String() string {
	return fmt.Sprintf("%s(%v)", OpName[int(e.fid>>32)], e.args)
}
