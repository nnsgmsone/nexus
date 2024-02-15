package scan

import (
	"bytes"
	"fmt"
	"io"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/vm/engine"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
	lua "github.com/yuin/gopher-lua"
)

const (
	DefaultBufferSize = 4 << 10
)

type luaState struct {
	l  *lua.LState
	ch chan *lua.LTable
}

type ScanOp struct {
	cols  []int
	lua   string
	state luaState
	r     engine.Reader
	rc    io.ReadCloser
	bat   *batch.Batch
	buf   *bytes.Buffer
	strs  []types.String
	vec   *vector.Vector
	proc  *process.Process
	msgs  []*pipeline.Message
}

func (o *ScanOp) String() string {
	return fmt.Sprintf("Extract lua = %s %v", o.lua, o.cols)
}

func (o *ScanOp) AddCtrlConsumer(_ ...int32) {
}

func (o *ScanOp) AddMessageConsumer(_ ...int32) {
}
