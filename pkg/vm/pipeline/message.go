package pipeline

import "github.com/nnsgmsone/nexus/pkg/container/batch"

func NewMessage(class uint16, tag uint16,
	consumer int32, bat *batch.Batch) *Message {
	msg := messagePool.Get().(*Message)
	msg.tag = tag
	msg.bat = bat
	msg.class = class
	msg.consumer = consumer
	return msg
}

func NewStopMessage() *Message {
	return NewMessage(CTRL, EMPTYFLG, -1, nil)
}

func (msg *Message) IsStopMessage() bool {
	return msg.class == CTRL && msg.bat == nil
}

func (msg *Message) Reset(class uint16, tag uint16,
	consumer int32, bat *batch.Batch) {
	msg.tag = tag
	msg.bat = bat
	msg.class = class
	msg.consumer = consumer
}
