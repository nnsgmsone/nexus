package plan

import (
	"fmt"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/engine"
)

const (
	Scan_Scope = iota
	Limit_Scope
	Order_Scope
	Group_Scope
	Import_Scope
	Filter_Scope
	Extract_Scope
	Projection_Scope
)

const (
	StopSignal = iota
)

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

type OrderBySpec struct {
	E    expr.Expr
	Type Direction
}

type Aggregate struct {
	Name  string
	FName string
	Agg   expr.Expr
	Es    []expr.Expr
}

type Limit struct {
	Limit int64
}

type Order struct {
	Limit  int64
	Orders []OrderBySpec
}

type Group struct {
	GroupBy []expr.Expr
	AggList []Aggregate
}

type Import struct {
	Paths []string
}

type Filter struct {
	Filter expr.Expr
}

type Projection struct {
	ProjectionList []expr.Expr
}

type Extract struct {
	// E is the expression(columnname) to extract.
	E    expr.Expr
	Lua  string
	Cols []int
}

type Scan struct {
}

type ScopeAttribute struct {
	// ID is an identifier for this attribute, which is unique across all
	// the attributes in the query.
	ID   uint32
	Name string
	Type types.Type
}

type State struct {
	// applyedRule is a bitmap of the rules that have been applied to this scope
	applyedRule uint64
}

type Scope struct {
	ScopeType  int
	state      State
	Scan       *Scan
	Limit      *Limit
	Order      *Order
	Group      *Group
	Import     *Import
	Filter     *Filter
	Extract    *Extract
	Projection *Projection

	Signals []*Scope

	Parent   *Scope
	Children []*Scope
	Attrs    []ScopeAttribute
}

type Plan struct {
	Root *Scope
}

type Build struct {
	id     uint32
	spl    string
	fs     vfs.FS
	scopes []Scope
	e      engine.Engine
}

type Rule interface {
	// ID returns the unique identifier for the rule.
	ID() uint64
	Name() string
	Apply(*Scope) *Scope
}

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "ASC",
	Descending:       "DESC",
}

var defaultAttribute = ScopeAttribute{
	Name: "default",
	Type: types.StringType,
}

func (i Direction) String() string {
	if i < 0 || i > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", i)
	}
	return directionName[i]
}

func (n *Scan) String() string {
	return ""
}

func (n *Import) String() string {
	var s string

	s = "IMPORT "
	for i, path := range n.Paths {
		if i > 0 {
			s += ", "
		}
		s += path
	}
	return s
}

func (n *Extract) String() string {
	return fmt.Sprintf("extract lua = %s, %v, %s", n.Lua, n.Cols, n.E)
}

func (n *Limit) String() string {
	return fmt.Sprintf("Limit %v", n.Limit)
}

func (n *Filter) String() string {
	return fmt.Sprintf("where %s", n.Filter)
}

func (n *Projection) String() string {
	var s string

	s = "eval "
	for i, n := range n.ProjectionList {
		if i > 0 {
			s += ", "
		}
		s += n.String()
	}
	return s

}

func (n *Group) String() string {
	s := fmt.Sprintf("STATS ")
	for i, agg := range n.AggList {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s(%s) As %s", agg.FName, agg.Es, agg.Name)
	}
	if len(n.GroupBy) > 0 {
		s += " by "
		for i := range n.GroupBy {
			if i > 0 {
				s += ", "
			}
			s += n.GroupBy[i].String()
		}
	}
	return s
}

func (n *Order) String() string {
	var s string

	s = "SORT BY "
	if n.Limit > 0 {
		s += fmt.Sprintf("%v ", n.Limit)
	}
	for i := range n.Orders {
		if i > 0 {
			s += ", "
		}
		s += n.Orders[i].String()
	}
	return s

}

func (n OrderBySpec) String() string {
	var s string

	s += n.E.String()
	if n.Type != DefaultDirection {
		s = " " + n.Type.String()
	}
	return s
}
