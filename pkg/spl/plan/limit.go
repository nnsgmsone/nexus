package plan

import (
	"go/constant"

	"github.com/nnsgmsone/nexus/pkg/spl/tree"
)

func (b *Build) buildLimitScope(in *Scope, stmt *tree.Limit) (*Scope, error) {
	out := b.newScope(in, Limit_Scope)
	limit, _ := constant.Int64Val(stmt.Count.Value)
	out.Limit = &Limit{Limit: limit}
	for _, attr := range in.Attrs {
		out.Attrs = append(out.Attrs, attr)
	}
	return out, nil
}
