package filter

import (
	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/util"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

func New(expr expr.Expr, proc *process.Process) *FilterOp {
	return &FilterOp{expr: expr, proc: proc}
}

func (o *FilterOp) Specialize() error {
	o.bats = make([]*batch.Batch, 1)
	o.msgs = make([]*pipeline.Message, 1)
	return o.expr.Specialize(o.proc)
}

func (o *FilterOp) Exec(msg *pipeline.Message) ([]*pipeline.Message, int, error) {
	if msg == nil {
		return nil, pipeline.END, nil
	}
	o.bats[0] = msg.GetBatch()
	vec, err := o.expr.Eval(o.bats, o.proc)
	if err != nil {
		return nil, pipeline.END, nil
	}
	vs := vector.GetColumnValue[bool](vec)
	if vec.IsConst() {
		if !vs[0] {
			return nil, pipeline.FILL, nil
		}
		o.msgs[0] = util.NewDataMessage(o.bats[0])
		return o.msgs, pipeline.FILL, nil
	}
	o.sels = o.sels[:0]
	for i, v := range vs {
		if v {
			o.sels = append(o.sels, uint32(i))
		}
	}
	if len(o.sels) == 0 {
		return nil, pipeline.FILL, nil
	}
	o.bats[0].Shrink(o.sels)
	o.msgs[0] = util.NewDataMessage(o.bats[0])
	return o.msgs, pipeline.FILL, nil
}

func (o *FilterOp) GetExecFunc() func(*pipeline.Message) ([]*pipeline.Message, int, error) {
	return o.Exec
}

func (o *FilterOp) Free() error {
	return nil
}
