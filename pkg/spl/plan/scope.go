package plan

import (
	"fmt"
	"strings"
)

func (s *Scope) String() string {
	switch s.ScopeType {
	case Scan_Scope:
		return "| " + s.Scan.String() + outputAttributes(s.Attrs)
	case Import_Scope:
		return "| " + s.Import.String()
	case Limit_Scope:
		return s.Children[0].String() + " | " + s.Limit.String() + outputAttributes(s.Attrs)
	case Order_Scope:
		return s.Children[0].String() + " | " + s.Order.String() + outputAttributes(s.Attrs)
	case Group_Scope:
		return s.Children[0].String() + " | " + s.Group.String() + outputAttributes(s.Attrs)
	case Filter_Scope:
		return s.Children[0].String() + " | " + s.Filter.String() + outputAttributes(s.Attrs)
	case Projection_Scope:
		return s.Children[0].String() + " | " + s.Projection.String() + outputAttributes(s.Attrs)
	case Extract_Scope:
		return s.Children[0].String() + " | " + s.Extract.String() + outputAttributes(s.Attrs)
	default:
		panic(fmt.Errorf("unsupport scope: %v", s.ScopeType))
	}
}

func (s *Scope) applyRule(r Rule) *Scope {
	if s.state.applyedRule&r.ID() != 0 {
		return s
	}
	s = r.Apply(s)
	s.state.applyedRule |= r.ID()
	return s
}

func outputAttributes(attrs []ScopeAttribute) string {
	var s string

	s = "("
	for i, attr := range attrs {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%v:%s:%s", attr.ID, attr.Name, attr.Type.String())
	}
	s += ")"
	return s
}

func (s *Scope) addParent(parent *Scope) {
	parent.Parent, s.Parent = s.Parent, parent
	parent.Children[0] = s
}

func (s *Scope) findAttribute(name string) (*ScopeAttribute, error) {
	idx := -1
	for i := range s.Attrs {
		if strings.ToLower(s.Attrs[i].Name) == name {
			if idx != -1 {
				return nil, fmt.Errorf("ambiguous column '%s'", name)
			}
			idx = i
		}
	}
	if idx == -1 {
		return nil, fmt.Errorf("Cannot find column '%s'", name)
	}
	return &s.Attrs[idx], nil
}

func (s *Scope) findAttributeByID(id uint32) *ScopeAttribute {
	for i := range s.Attrs {
		if s.Attrs[i].ID == id {
			return &s.Attrs[i]
		}
	}
	return nil
}

// return true if the scope use the attribute id
func (s *Scope) hasAttribute(id uint32) bool {
	use := false
	for _, attr := range s.Attrs {
		if attr.ID == id {
			use = true
		}
	}
	if use {
		return true
	}
	fn := func(relID, colID uint32) (uint32, uint32) {
		if colID == id {
			use = true
		}
		return relID, colID
	}
	switch s.ScopeType {
	case Scan_Scope:
		s.Scan.Extract.E.IterateAllColExpr(fn)
	case Order_Scope:
		for i := range s.Order.Orders {
			s.Order.Orders[i].E.IterateAllColExpr(fn)
		}
	case Group_Scope:
		for i := range s.Group.GroupBy {
			s.Group.GroupBy[i].IterateAllColExpr(fn)
		}
		for i := range s.Group.AggList {
			for j := range s.Group.AggList[i].Es {
				s.Group.AggList[i].Es[j].IterateAllColExpr(fn)
			}
		}
	case Filter_Scope:
		s.Filter.Filter.IterateAllColExpr(fn)
	case Projection_Scope:
		for i := range s.Projection.ProjectionList {
			s.Projection.ProjectionList[i].IterateAllColExpr(fn)
		}
	case Extract_Scope:
		s.Extract.E.IterateAllColExpr(fn)
	}
	return use
}

func (s *Scope) findAttributeIndexByID(id uint32) int {
	for i := range s.Attrs {
		if s.Attrs[i].ID == id {
			return i
		}
	}
	return -1
}
