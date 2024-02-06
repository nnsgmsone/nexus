package tree

import "fmt"

type OrderBy struct {
	Limit  *Value
	Orders OrderList
}

func (n OrderBy) String() string {
	var s string

	s = "SORT BY "
	if n.Limit != nil {
		s += n.Limit.String() + " "
	}
	for i := range n.Orders {
		if i > 0 {
			s += ", "
		}
		s += n.Orders[i].String()
	}
	return s
}

type OrderList []*Order

type Order struct {
	Type Direction
	E    ExprStatement
}

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "ASC",
	Descending:       "DESC",
}

func (i Direction) String() string {
	if i < 0 || i > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", i)
	}
	return directionName[i]
}

func (n *Order) String() string {
	var s string

	s += n.E.String()
	if n.Type != DefaultDirection {
		s = " " + n.Type.String()
	}
	return s
}
