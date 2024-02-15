package emit

import (
	"github.com/nnsgmsone/nexus/pkg/container/batch"
)

type EmitOp struct {
	ctrls []int32
	emit  func(*batch.Batch) error
}

func (o *EmitOp) String() string {
	return "emit"
}

func (o *EmitOp) AddCtrlConsumer(ctrls ...int32) {
	o.ctrls = append(o.ctrls, ctrls...)
}

func (o *EmitOp) AddMessageConsumer(_ ...int32) {
}
