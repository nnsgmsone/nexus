package compile

import (
	"sync"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/spl/plan"
	"github.com/nnsgmsone/nexus/pkg/vm/engine"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type Operator interface {
	String() string
	Free() error
	Specialize() error
	AddCtrlConsumer(...int32)
	AddMessageConsumer(...int32)
	GetExecFunc() func(*pipeline.Message) ([]*pipeline.Message, int, error)
}

type Compile struct {
	mcpu int
	spl  string
	pn   *plan.Plan
	ops  []Operator
	e    engine.Engine
	wg   sync.WaitGroup
	ctx  compileContext
	proc *process.Process
	ws   *pipeline.Workers
	emit func(*batch.Batch) error
}

type compileContext struct {
	// The current scope.
	ops     []Operator
	tasks   []*pipeline.Task
	ws      []*pipeline.Worker
	signals map[*plan.Scope][]*pipeline.Task
}
