package plan

type remapColPos struct {
}

func NewRemapColPosRule() *remapColPos {
	return &remapColPos{}
}

func (r *remapColPos) ID() uint64 {
	return RemapColPosRuleID
}

func (r *remapColPos) Name() string {
	return "remap_colpos"
}

func (r *remapColPos) Apply(root *Scope) *Scope {
	fn := func(relPos uint32, colPos uint32) (uint32, uint32) {
		if root.ScopeType == Scan_Scope {
			return relPos, uint32(root.findAttributeIndexByID(colPos))
		}
		return relPos, uint32(root.Children[relPos].findAttributeIndexByID(colPos))
	}
	switch root.ScopeType {
	case Scan_Scope:
	case Group_Scope:
		for i := range root.Group.GroupBy {
			root.Group.GroupBy[i].IterateAllColExpr(fn)
		}
		for i := range root.Group.AggList {
			for j := range root.Group.AggList {
				root.Group.AggList[i].Es[j].IterateAllColExpr(fn)
			}
		}
	case Order_Scope:
		for i := range root.Order.Orders {
			root.Order.Orders[i].E.IterateAllColExpr(fn)
		}
	case Extract_Scope:
		root.Extract.E.IterateAllColExpr(fn)
	case Filter_Scope:
		root.Filter.Filter.IterateAllColExpr(fn)
	case Projection_Scope:
		for i := range root.Projection.ProjectionList {
			root.Projection.ProjectionList[i].IterateAllColExpr(fn)
		}
	}
	return root
}
