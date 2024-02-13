package plan

type stopPushdownRule struct {
}

func NewStopPushdownRule() *stopPushdownRule {
	return &stopPushdownRule{}
}

func (r *stopPushdownRule) ID() uint64 {
	return StopPushdownRuleID
}

func (r *stopPushdownRule) Name() string {
	return "stop_pushdown"
}

func (r *stopPushdownRule) Apply(root *Scope) *Scope {
	switch root.ScopeType {
	case Limit_Scope:
		r.pushDown(root, root)
		return root
	}
	return root
}

func (r *stopPushdownRule) pushDown(root, curr *Scope) {
	if curr.ScopeType == Scan_Scope {
		root.Signals = append(root.Signals, curr)
		return
	}
	for i := range curr.Children {
		r.pushDown(root, curr.Children[i])
	}
}
