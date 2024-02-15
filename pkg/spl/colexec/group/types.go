package group

import (
	"fmt"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/indextable"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/spl/colexec/agg"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type Aggregate struct {
	name string
	typ  types.Type
	args []types.Type

	agg agg.Agg
	es  []expr.Expr
}

type GroupOp struct {
	filled      bool
	needEval    bool
	producerNum int
	grps        []uint32
	sels        []uint32
	// new group's select list
	ngrpsels  []uint32
	aggs      []Aggregate
	groupBy   []expr.Expr
	proc      *process.Process
	idx       *indextable.Index
	bat       *batch.Batch
	bats      []*batch.Batch
	groupVecs []*vector.Vector
	aggVecs   [][]*vector.Vector
	msgs      []*pipeline.Message
}

func (o *GroupOp) String() string {
	s := fmt.Sprintf("STATS ")
	for i, agg := range o.aggs {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s(%s)", agg.name, agg.es)
	}
	if len(o.groupBy) > 0 {
		s += " by "
		for i := range o.groupBy {
			if i > 0 {
				s += ", "
			}
			s += o.groupBy[i].String()
		}
	}
	return s
}

func (o *GroupOp) AddCtrlConsumer(_ ...int32) {}

func (o *GroupOp) AddMessageConsumer(_ ...int32) {}
