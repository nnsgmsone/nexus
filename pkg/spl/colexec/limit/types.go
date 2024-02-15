package limit

import (
	"fmt"
)

type LimitOp struct {
	producerNum int
	seen        uint64
	limit       uint64
	ctrls       []int32
	consumers   []int32
}

func (o *LimitOp) String() string {
	return fmt.Sprintf("LIMIT %d", o.limit)
}

func (o *LimitOp) AddCtrlConsumer(ctrls ...int32) {
	o.ctrls = append(o.ctrls, ctrls...)
}

func (o *LimitOp) AddMessageConsumer(consumers ...int32) {
	o.consumers = append(o.consumers, consumers...)
}
