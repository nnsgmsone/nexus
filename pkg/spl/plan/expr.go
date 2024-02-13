package plan

import (
	"fmt"
	"strings"

	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/spl/tree"
)

func (b *Build) buildExpr(in *Scope, expr tree.ExprStatement) (expr.Expr, error) {
	switch e := expr.(type) {
	case *tree.Value:
		return b.buildValue(in, e)
	case *tree.FuncExpr:
		return b.buildFuncExpr(in, e)
	case *tree.ParenExpr:
		return b.buildExpr(in, e.E)
	case tree.ColumnName:
		return b.buildColumnName(in, string(e.Path))
	default:
		return nil, fmt.Errorf("Unsupport expression '%v'", expr)
	}
}

func (b *Build) buildValue(in *Scope, val *tree.Value) (expr.Expr, error) {
	return expr.NewConstExpr(val.Isnull, val.Value)
}

func (b *Build) buildFuncExpr(in *Scope, fe *tree.FuncExpr) (expr.Expr, error) {
	args := make([]expr.Expr, len(fe.Args))
	for i, arg := range fe.Args {
		e, err := b.buildExpr(in, arg)
		if err != nil {
			return nil, err
		}
		args[i] = e
	}
	return expr.NewFuncExpr(fe.Name, args)
}

func (b *Build) buildColumnName(in *Scope, name string) (expr.Expr, error) {
	attr, err := in.findAttribute(strings.ToLower(name))
	if err != nil {
		return nil, err
	}
	return expr.NewColExpr(0, attr.ID, attr.Type), nil
}

func (b *Build) generateAttributeIDByExpr(e expr.Expr) uint32 {
	colPoses := make([]uint32, 0, 1)
	e.IterateAllColExpr(func(relPos, colPos uint32) (uint32, uint32) {
		colPoses = append(colPoses, colPos)
		return relPos, colPos
	})
	if _, ok := e.(*expr.FuncExpr); ok {
		return b.allocID()
	}
	return colPoses[0]
}

func mergeExprsWithAnd(exprs []expr.Expr) expr.Expr {
	switch len(exprs) {
	case 0:
		panic("empty expression list")
	case 1:
		return exprs[0]
	default:
		expr, err := expr.NewFuncExpr("and", []expr.Expr{exprs[0], mergeExprsWithAnd(exprs[1:])})
		if err != nil {
			panic(err)
		}
		return expr
	}
}
