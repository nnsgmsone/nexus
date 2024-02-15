package plan

type extractPushDownRule struct {
}

func NewExtractPushDownRule() *extractPushDownRule {
	return &extractPushDownRule{}
}

func (r *extractPushDownRule) ID() uint64 {
	return ExtractPushDownRuleID
}

func (r *extractPushDownRule) Name() string {
	return "ExtractPushDownRule"
}

func (r *extractPushDownRule) Apply(root *Scope) *Scope {
	if len(root.Children) == 1 && root.Children[0].ScopeType == Extract_Scope &&
		len(root.Children[0].Children) == 1 && root.Children[0].Children[0].ScopeType == Scan_Scope {
		// scan -> extract ->  root
		scanScope := &Scope{
			ScopeType: Scan_Scope,
			Attrs:     root.Children[0].Attrs,
			Scan: &Scan{
				Extract: root.Children[0].Extract,
			},
		}
		root = dupScope(scanScope, root)
	}
	return root
}
