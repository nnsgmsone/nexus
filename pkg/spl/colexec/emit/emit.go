package emit

import (
	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/util"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
)

func New(emit func(*batch.Batch) error) *EmitOp {
	return &EmitOp{emit: emit}
}

func (o *EmitOp) Specialize() error {
	return nil
}

func (o *EmitOp) Exec(msg *pipeline.Message) ([]*pipeline.Message, int, error) {
	if msg == nil {
		return nil, pipeline.END, nil
	}
	if msg.GetBatch().Rows() == 0 {
		return nil, pipeline.END, nil
	}
	if err := o.emit(msg.GetBatch()); err != nil {
		return util.NewCtrlMessages(o.ctrls, nil), pipeline.END, nil
	}
	return nil, pipeline.FILL, nil
}

func (o *EmitOp) GetExecFunc() func(*pipeline.Message) ([]*pipeline.Message, int, error) {
	return o.Exec
}

func (o *EmitOp) Free() error {
	return nil
}
