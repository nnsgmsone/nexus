package tree

type Name string

func (n Name) String() string {
	return string(n)
}

type NameList []Name

func (n NameList) String() string {
	var s string

	for i := range n {
		if i > 0 {
			s += ", "
		}
		s += n[i].String()
	}
	return s
}

type ColumnName struct {
	Path Name
}

func (n ColumnName) String() string {
	return n.Path.String()
}

type ColumnNameList []ColumnName

func (n ColumnNameList) String() string {
	var s string

	for i := range n {
		if i > 0 {
			s += ", "
		}
		s += n[i].String()
	}
	return s
}
