package filter

import (
	"fmt"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type FilterOp struct {
	sels []uint32
	expr expr.Expr
	bats []*batch.Batch
	proc *process.Process
	msgs []*pipeline.Message
}

func (o *FilterOp) String() string {
	return fmt.Sprintf("Filter %s", o.expr)
}

func (o *FilterOp) AddCtrlConsumer(_ ...int32) {}

func (o *FilterOp) AddMessageConsumer(_ ...int32) {}
