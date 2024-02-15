package load

import (
	"github.com/nnsgmsone/nexus/pkg/vm/engine"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type ImportOp struct {
	paths []string
	e     engine.Engine
	proc  *process.Process
}

func (o *ImportOp) String() string {
	var s string

	s = "IMPORT "
	for i, path := range o.paths {
		if i > 0 {
			s += ", "
		}
		s += path
	}
	return s
}

func (o *ImportOp) AddCtrlConsumer(_ ...int32) {}

func (o *ImportOp) AddMessageConsumer(_ ...int32) {}
