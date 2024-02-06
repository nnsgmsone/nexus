package tree

type TableName struct {
	N ColumnName
}

func (n *TableName) String() string {
	return n.N.String()
}
