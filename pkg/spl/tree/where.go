package tree

type Where struct {
	E ExprStatement
}

func (n *Where) String() string {
	return "WHERE " + n.E.String()
}
