package plan

import (
	"github.com/nnsgmsone/nexus/pkg/spl/tree"
)

func (b *Build) buildEvalScope(in *Scope, stmt *tree.Eval) (*Scope, error) {
	out := b.newScope(in, Projection_Scope)
	out.Projection = new(Projection)
	for i := range stmt.Es {
		e, err := b.buildExpr(in, stmt.Es[i].E)
		if err != nil {
			return nil, err
		}
		out.Projection.ProjectionList = append(out.Projection.ProjectionList, e)
		as := string(stmt.Es[i].As)
		if len(as) == 0 {
			as = stmt.Es[i].E.String()
		}
		out.Attrs = append(out.Attrs, ScopeAttribute{
			Name: as,
			Type: e.ResultType(),
			ID:   b.generateAttributeIDByExpr(e),
		})
	}
	return out, nil
}
