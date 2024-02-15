package limit

import (
	"github.com/nnsgmsone/nexus/pkg/util"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
)

func New(limit uint64, producerNum int) *LimitOp {
	return &LimitOp{limit: limit, producerNum: producerNum}
}

func (o *LimitOp) Specialize() error {
	return nil
}

func (o *LimitOp) Exec(msg *pipeline.Message) ([]*pipeline.Message, int, error) {
	if msg == nil {
		o.producerNum--
		if o.producerNum == 0 {
			return nil, pipeline.END, nil
		}
		return nil, pipeline.FILL, nil
	}
	if o.seen >= o.limit {
		return util.NewCtrlMessages(o.ctrls, nil), pipeline.END, nil
	}
	rows := msg.GetBatch().Rows()
	newSeen := o.seen + uint64(rows)
	if newSeen >= o.limit {
		msg.GetBatch().SetRows(int(o.limit - o.seen))
	}
	o.seen = newSeen
	return util.NewDataMessages(o.consumers, msg.GetBatch()), pipeline.FILL, nil
}

func (o *LimitOp) GetExecFunc() func(*pipeline.Message) ([]*pipeline.Message, int, error) {
	return o.Exec
}

func (o *LimitOp) Free() error {
	return nil
}
