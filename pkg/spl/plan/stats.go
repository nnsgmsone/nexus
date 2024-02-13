package plan

import (
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/spl/tree"
)

func (b *Build) buildStatsScope(in *Scope, stmt *tree.Stats) (*Scope, error) {
	out := b.newScope(in, Group_Scope)
	out.Group = new(Group)
	if len(stmt.By) > 0 {
		for _, col := range stmt.By {
			e, err := b.buildExpr(in, tree.ColumnName{Path: col.Path})
			if err != nil {
				return nil, err
			}
			out.Group.GroupBy = append(out.Group.GroupBy, e)
			out.Attrs = append(out.Attrs, ScopeAttribute{
				Type: e.ResultType(),
				Name: string(col.Path),
				ID:   b.generateAttributeIDByExpr(e),
			})
		}
	}
	for _, s := range stmt.Ss {
		exprs := make([]expr.Expr, len(s.Es))
		if len(s.Es) == 0 {
			e, err := b.buildExpr(in, tree.ColumnName{Path: tree.Name(in.Attrs[0].Name)})
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, e)
		} else {
			for i := range s.Es {
				e, err := b.buildExpr(in, s.Es[i])
				if err != nil {
					return nil, err
				}
				exprs[i] = e
			}
		}
		agg, err := expr.NewFuncExpr(string(s.F), exprs)
		if err != nil {
			return nil, err
		}
		as := string(s.As)
		if len(as) == 0 {
			as = s.String()
		}
		out.Group.AggList = append(out.Group.AggList, Aggregate{
			Agg:   agg,
			Es:    exprs,
			Name:  as,
			FName: string(s.F),
		})
		out.Attrs = append(out.Attrs, ScopeAttribute{
			Name: as,
			Type: agg.ResultType(),
			ID:   b.allocID(),
		})
	}
	return out, nil
}
