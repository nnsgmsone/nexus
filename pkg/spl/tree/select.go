package tree

type CommandStatement interface {
	Statement
	commandStatement()
}

func (*Eval) commandStatement()    {}
func (*Limit) commandStatement()   {}
func (*Stats) commandStatement()   {}
func (*Where) commandStatement()   {}
func (*OrderBy) commandStatement() {}
func (*Extract) commandStatement() {}
func (*Import) commandStatement()  {}

type Select struct {
	Cs []CommandStatement
}

func (n *Select) String() string {
	var s string

	for i, c := range n.Cs {
		if i > 0 {
			s += " | "
		}
		s += c.String()
	}
	return s
}
