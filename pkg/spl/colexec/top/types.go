package top

import (
	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/spl/compare"
	"github.com/nnsgmsone/nexus/pkg/spl/plan"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type TopOp struct {
	producerNum int
	limit       int64
	sels        []uint32
	bat         *batch.Batch
	bats        []*batch.Batch
	vecs        []*vector.Vector
	proc        *process.Process
	orders      []plan.OrderBySpec
	cmps        []compare.Compare
	attrs       []plan.ScopeAttribute
	msgs        []*pipeline.Message
}

func (op *TopOp) String() string {
	var s string

	s = "Top by "
	for i := range op.orders {
		if i > 0 {
			s += ", "
		}
		s += op.orders[i].String()
	}
	return s
}

func (o *TopOp) AddCtrlConsumer(_ ...int32) {}

func (o *TopOp) AddMessageConsumer(_ ...int32) {}

func (o *TopOp) compare(vi, vj int, i, j int64) int {
	for k := 0; k < len(o.orders); k++ {
		if r := o.cmps[k].Compare(vi, vj, i, j); r != 0 {
			return r
		}
	}
	return 0
}

// maximum heap
func (o *TopOp) Len() int {
	return len(o.sels)
}

func (o *TopOp) Less(i, j int) bool {
	return o.compare(0, 0, int64(o.sels[i]), int64(o.sels[j])) > 0
}

func (o *TopOp) Swap(i, j int) {
	o.sels[i], o.sels[j] = o.sels[j], o.sels[i]
}

func (o *TopOp) Push(x interface{}) {
	o.sels = append(o.sels, x.(uint32))
}

func (o *TopOp) Pop() interface{} {
	n := len(o.sels) - 1
	x := o.sels[n]
	o.sels = o.sels[:n]
	return x
}
