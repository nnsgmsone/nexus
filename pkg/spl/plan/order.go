package plan

import (
	"go/constant"

	"github.com/nnsgmsone/nexus/pkg/spl/tree"
)

func (b *Build) buildOrderByScope(in *Scope, stmt *tree.OrderBy) (*Scope, error) {
	out := b.newScope(in, Order_Scope)
	limit := int64(-1)
	if stmt.Limit != nil {
		limit, _ = constant.Int64Val(stmt.Limit.Value)
	}
	out.Order = new(Order)
	out.Order.Limit = limit
	for _, ord := range stmt.Orders {
		e, err := b.buildExpr(in, ord.E)
		if err != nil {
			return nil, err
		}
		out.Order.Orders = append(out.Order.Orders, OrderBySpec{
			E:    e,
			Type: Direction(ord.Type),
		})
	}
	for _, attr := range in.Attrs {
		out.Attrs = append(out.Attrs, attr)
	}
	return out, nil
}
