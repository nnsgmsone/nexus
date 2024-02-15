package projection

import (
	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type ProjectionOp struct {
	exprs []expr.Expr
	bat   *batch.Batch
	bats  []*batch.Batch
	proc  *process.Process
	msgs  []*pipeline.Message
}

func (op *ProjectionOp) String() string {
	var str string

	str = "eval "
	for i, e := range op.exprs {
		if i > 0 {
			str += ", "
		}
		str += e.String()
	}
	return str
}

func (op *ProjectionOp) AddCtrlConsumer(_ ...int32) {}

func (op *ProjectionOp) AddMessageConsumer(_ ...int32) {}
