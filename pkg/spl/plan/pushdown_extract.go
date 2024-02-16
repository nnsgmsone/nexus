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
	if root.ScopeType == Extract_Scope &&
		len(root.Children) == 1 && root.Children[0].ScopeType == Scan_Scope {
		// scan -> extract -> parent ==> scan -> parent
		scanScope := new(Scope)
		*scanScope = *root.Children[0]
		scanScope.Attrs = root.Attrs
		scanScope.Scan = &Scan{
			Extract: root.Extract,
		}
		scanScope.Parent = root.Parent
		if root.Parent != nil {
			root.Parent.Children = []*Scope{scanScope}
		}
		root = scanScope
	}
	return root
}
