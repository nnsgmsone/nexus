package pipeline

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/nnsgmsone/nexus/pkg/container/batch"
)

const (
	FILL = iota
	EVAL
	END
)

const (
	DATA = iota
	CTRL
)

// system-defined tag
const (
	EMPTYFLG = 0x0
	// Indicates that this message must be forced to make a copy and cannot be used directly
	NEEDDUP = 0x01
)

type Workers struct {
	sync.RWMutex
	ws     []*Worker
	ctx    context.Context
	cancel context.CancelFunc
	mp     map[uuid.UUID]*Worker
	pkts   []*Packet
}

type Worker struct {
	err       error
	ws        *Workers
	id        uuid.UUID
	buffer    []*Packet
	ch        chan *Packet
	ctrlCh    chan *Packet
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	tasks     map[uuid.UUID]*Task
	evalTasks map[uuid.UUID]*Task
	fillTasks map[uuid.UUID]*Task

	ctrlCond  sync.Cond
	ctrlLock  sync.Mutex
	ctrlStop  chan struct{}
	ctrlQueue *ctrlPacketQueue
}

type Packet struct {
	msg    *Message
	taskID uuid.UUID
	wg     sync.WaitGroup
}

type Message struct {
	class    uint16
	tag      uint16
	consumer int32
	bat      *batch.Batch
}

type Task struct {
	state         int
	id            uuid.UUID
	wid           uuid.UUID
	msg           *Message
	consumers     []*Task
	consumersType []int
	wg            *sync.WaitGroup
	exec          func(*Message) ([]*Message, int, error)
}

type ctrlPacketQueue struct {
	sync.Mutex
	ctrls []*ctrlPacket
}

type ctrlPacket struct {
	pkt  *Packet
	dest *Worker
}

func (t *Task) ID() uuid.UUID {
	return t.id
}

func (msg *Message) GetClass() uint16 {
	return msg.class
}

func (msg *Message) GetBatch() *batch.Batch {
	return msg.bat
}

func (msg *Message) SetBatch(bat *batch.Batch) {
	msg.bat = bat
}

func (pkt *Packet) hasTag(tag uint16) bool {
	return pkt != nil && pkt.msg != nil && pkt.msg.tag&tag != 0
}

func (pkt *Packet) clearTag(tag uint16) {
	pkt.msg.tag = pkt.msg.tag &^ tag
}
