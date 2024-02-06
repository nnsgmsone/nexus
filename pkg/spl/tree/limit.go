package tree

import "fmt"

type Limit struct {
	Count *Value
}

func (n *Limit) String() string {
	return fmt.Sprintf("LIMIT %s", n.Count)
}
