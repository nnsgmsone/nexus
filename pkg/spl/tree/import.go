package tree

type Import struct {
	Paths []*Value
}

func (n Import) String() string {
	var s string

	s = "Import "
	for i := range n.Paths {
		if i > 0 {
			s += ", "
		}
		s += n.Paths[i].String()
	}
	return s
}
