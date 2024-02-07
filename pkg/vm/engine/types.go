package engine

import (
	"bytes"

	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

type Reader interface {
	Specialize() error
	Read(*vector.Vector, *bytes.Buffer) error
}

type Engine interface {
	Clean() error
	Write(*vector.Vector) error
	NewReader(*process.Process) (Reader, error)
}
