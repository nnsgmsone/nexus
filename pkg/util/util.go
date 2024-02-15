package util

import (
	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
)

func NewDataMessage(bat *batch.Batch) *pipeline.Message {
	return pipeline.NewMessage(pipeline.DATA, pipeline.NEEDDUP, -1, bat)
}

func NewCtrlMessages(consumers []int32, bat *batch.Batch) []*pipeline.Message {
	msgs := make([]*pipeline.Message, len(consumers))
	for i, consumer := range consumers {
		msgs[i] = pipeline.NewMessage(pipeline.CTRL, 0, consumer, bat)
	}
	return msgs
}

func NewDataMessages(consumers []int32, bat *batch.Batch) []*pipeline.Message {
	msgs := make([]*pipeline.Message, len(consumers))
	for i, consumer := range consumers {
		msgs[i] = pipeline.NewMessage(pipeline.DATA, pipeline.NEEDDUP, consumer, bat)
	}
	return msgs
}
