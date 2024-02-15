package exchange

import (
	"fmt"

	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
)

const (
	RoundRobin = iota
	Hash
)

type ExchangeOp struct {
	idx       int
	typ       uint32
	consumers []int32
	msgs      []*pipeline.Message
}

func (op *ExchangeOp) String() string {
	if op.typ == RoundRobin {
		return fmt.Sprintf("RoundRobin: %v", op.consumers)
	} else {
		return fmt.Sprintf("Hash: %v", op.consumers)
	}
}

func (o *ExchangeOp) AddCtrlConsumer(_ ...int32) {}

func (o *ExchangeOp) AddMessageConsumer(consumers ...int32) {
	o.consumers = append(o.consumers, consumers...)
}
