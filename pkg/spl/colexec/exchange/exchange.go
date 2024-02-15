package exchange

import "github.com/nnsgmsone/nexus/pkg/vm/pipeline"

func New(typ uint32) *ExchangeOp {
	return &ExchangeOp{typ: typ}
}

func (o *ExchangeOp) Specialize() error {
	o.idx = 0
	o.msgs = make([]*pipeline.Message, 1)
	return nil
}

func (o *ExchangeOp) Exec(msg *pipeline.Message) ([]*pipeline.Message, int, error) {
	if msg == nil {
		return nil, pipeline.END, nil
	}
	o.msgs[0] = pipeline.NewMessage(pipeline.DATA,
		pipeline.NEEDDUP, o.consumers[o.idx%len(o.consumers)], msg.GetBatch())
	o.idx++
	return o.msgs, pipeline.FILL, nil
}

func (o *ExchangeOp) GetExecFunc() func(*pipeline.Message) ([]*pipeline.Message, int, error) {
	return o.Exec
}

func (o *ExchangeOp) Free() error {
	return nil
}
