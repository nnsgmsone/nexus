package order

import (
	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/spl/plan"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type OrderOp struct {
	bat    *batch.Batch
	proc   *process.Process
	orders []plan.OrderBySpec
	msgs   []*pipeline.Message
	attrs  []plan.ScopeAttribute
}
