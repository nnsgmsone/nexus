package plan

func (b *Build) buildScanScope() (*Scope, error) {
	attrs := make([]ScopeAttribute, 1)
	attrs[0] = defaultAttribute
	attrs[0].ID = b.allocID()
	s := b.allocScope()
	s.Attrs = attrs
	s.ScopeType = Scan_Scope
	s.Scan = &Scan{}
	return s, nil
}
