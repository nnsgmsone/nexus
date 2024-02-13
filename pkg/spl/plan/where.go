package plan

import (
	"fmt"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/spl/tree"
)

func (b *Build) buildWhereScope(in *Scope, stmt *tree.Where) (*Scope, error) {
	e, err := b.buildExpr(in, stmt.E)
	if err != nil {
		return nil, err
	}
	if !e.ResultType().IsType(types.T_bool) {
		return nil, fmt.Errorf("Illegal expression '%s' in where", stmt)
	}
	return b.newFilterScope(in, e), nil
}

func (b *Build) newFilterScope(in *Scope, filter expr.Expr) *Scope {
	out := b.newScope(in, Filter_Scope)
	out.Filter = new(Filter)
	out.Filter.Filter = filter
	for _, attr := range in.Attrs {
		out.Attrs = append(out.Attrs, attr)
	}
	return out
}

func newFilterScope(in *Scope, filter expr.Expr) *Scope {
	out := newScope(in, Filter_Scope)
	out.Filter = new(Filter)
	out.Filter.Filter = filter
	for _, attr := range in.Attrs {
		out.Attrs = append(out.Attrs, attr)
	}
	return out
}
