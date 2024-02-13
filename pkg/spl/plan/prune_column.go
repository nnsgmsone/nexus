package plan

type pruneColumn struct {
}

func NewPruneColumn() *pruneColumn {
	return &pruneColumn{}
}

func (r *pruneColumn) ID() uint64 {
	return PruneColumnRuleID
}

func (r *pruneColumn) Name() string {
	return "prune_column"
}

func (r *pruneColumn) Apply(root *Scope) *Scope {
	for i := 0; i < len(root.Attrs); i++ {
		if root.Parent != nil && !root.Parent.hasAttribute(root.Attrs[i].ID) {
			root.Attrs = append(root.Attrs[:i], root.Attrs[i+1:]...)
			i--
		}
	}
	for i := range root.Children {
		r.Apply(root.Children[i])
	}
	return root
}
