package agg

import (
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type numeric interface {
	constraints.Integer | constraints.Float
}

type Agg interface {
	Grows(int)
	Eval() (*vector.Vector, error)
	Specialize(*process.Process) error
	Merge(Agg, []uint32, []uint32) error
	BulkFill(uint32, []*vector.Vector) error
	Fill([]uint32, []uint32, []*vector.Vector) error

	Load(*vector.Vector) error
	Save() (*vector.Vector, error)
}
