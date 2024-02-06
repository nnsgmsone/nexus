package tree

import (
	"fmt"
	"go/constant"
)

type ExprStatement interface {
	Statement
	exprStatement()
}

type Value struct {
	Isnull bool
	Value  constant.Value
}

type FuncExpr struct {
	Name string
	Args ExprStatements
}

type ParenExpr struct {
	E ExprStatement
}

type ExprStatements []ExprStatement

func (*Value) exprStatement()         {}
func (*FuncExpr) exprStatement()      {}
func (*ParenExpr) exprStatement()     {}
func (ExprStatements) exprStatement() {}
func (ColumnName) exprStatement()     {}
func (ColumnNameList) exprStatement() {}

func (e *Value) String() string {
	if e.Isnull {
		return "NULL"
	}
	return e.Value.String()
}

func (e *FuncExpr) String() string {
	return fmt.Sprintf("%s(%s)", e.Name, e.Args)
}

func (e *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", e.E)
}

func (es ExprStatements) String() string {
	var s string

	for i := range es {
		if i > 0 {
			s += ", "
		}
		s += es[i].String()
	}
	return s
}
