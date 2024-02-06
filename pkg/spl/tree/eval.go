package tree

import "fmt"

type Eval struct {
	Es EvalExprList
}

type EvalExprList []EvalExpr

type EvalExpr struct {
	As Name
	E  ExprStatement
}

func (n *Eval) String() string {
	return fmt.Sprintf("EVAL %s", n.Es)
}

func (ns EvalExprList) String() string {
	var s string

	for i, n := range ns {
		if i > 0 {
			s += ", "
		}
		s += n.String()
	}
	return s
}

func (n EvalExpr) String() string {
	return fmt.Sprintf("%s = %s", n.As, n.E)
}
