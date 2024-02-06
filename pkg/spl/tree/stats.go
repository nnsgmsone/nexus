package tree

import "fmt"

type Stats struct {
	Ss StatList
	By ColumnNameList
}

type StatList []Stat

type Stat struct {
	As Name
	F  Name // func name
	Es ExprStatements
}

func (n *Stats) String() string {
	s := fmt.Sprintf("STATS ")
	s += n.Ss.String()
	if len(n.By) > 0 {
		s += " by " + n.By.String()
	}
	return s
}

func (ns StatList) String() string {
	var s string

	for i, n := range ns {
		if i > 0 {
			s += ", "
		}
		s += n.String()
	}
	return s
}

func (n Stat) String() string {
	switch {
	case len(n.Es) == 0 && len(n.As) == 0:
		return fmt.Sprintf("%s()", n.F)
	case len(n.Es) != 0 && len(n.As) == 0:
		return fmt.Sprintf("%s(%s)", n.F, n.Es)
	case len(n.Es) == 0 && len(n.As) != 0:
		return fmt.Sprintf("%s() As %s", n.F, n.As)
	default:
		return fmt.Sprintf("%s(%s) As %s", n.F, n.Es, n.As)
	}
}
