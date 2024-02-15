package projection

import (
	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/util"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

func New(exprs []expr.Expr, proc *process.Process) *ProjectionOp {
	return &ProjectionOp{
		exprs: exprs,
		proc:  proc,
	}
}

func (o *ProjectionOp) Specialize() error {
	o.bats = make([]*batch.Batch, 1)
	o.msgs = make([]*pipeline.Message, 1)
	o.bat = batch.New(len(o.exprs), o.proc.FS())
	for i := range o.exprs {
		if err := o.exprs[i].Specialize(o.proc); err != nil {
			return err
		}
	}
	return nil
}

func (o *ProjectionOp) Exec(msg *pipeline.Message) ([]*pipeline.Message, int, error) {
	if msg == nil {
		return nil, pipeline.END, nil
	}
	o.bats[0] = msg.GetBatch()
	for i := range o.exprs {
		vec, err := o.exprs[i].Eval(o.bats, o.proc)
		if err != nil {
			return nil, pipeline.END, err
		}
		o.bat.SetVector(i, vec)
	}
	o.bat.SetRows(o.bats[0].Rows())
	o.msgs[0] = util.NewDataMessage(o.bat)
	return o.msgs, pipeline.FILL, nil
}

func (o *ProjectionOp) GetExecFunc() func(*pipeline.Message) ([]*pipeline.Message, int, error) {
	return o.Exec
}

func (o *ProjectionOp) Free() error {
	return nil
}
