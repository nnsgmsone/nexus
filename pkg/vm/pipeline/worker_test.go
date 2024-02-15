package pipeline

import (
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/vfs"
)

var fs = vfs.NewMemFS()

type outputOp struct {
}

type testScanOp struct {
	cnt int
	bat *batch.Batch
}

type testOrderOp struct {
}

type testMergeOrderOp struct {
	state       int
	numProducer int
	start       int
	sels        []uint32
	bat         *batch.Batch
	obat        *batch.Batch
}

func TestWorker(t *testing.T) {
	var wg sync.WaitGroup

	ncpu := runtime.NumCPU()
	workers := NewWorkers(ncpu)
	scanOps := make([]*testScanOp, ncpu)
	for i := range scanOps {
		scanOps[i] = new(testScanOp)
	}
	orderOps := make([]*testOrderOp, ncpu)
	for i := range orderOps {
		orderOps[i] = new(testOrderOp)
	}
	mergeOp := new(testMergeOrderOp)
	mergeOp.numProducer = ncpu
	ws := workers.GetWorkers()
	scanTasks := make([]*Task, ncpu)
	for i := range scanOps {
		scanTasks[i] = NewTask(EVAL, &wg, scanOps[i].Exec)
		ws[i].AddTask(scanTasks[i])
	}
	orderTasks := make([]*Task, ncpu)
	for i := range orderTasks {
		orderTasks[i] = NewTask(FILL, &wg, orderOps[i].Exec)
		ws[i].AddTask(orderTasks[i])
	}
	mergeTask := NewTask(FILL, &wg, mergeOp.Exec)
	ws[0].AddTask(mergeTask)
	for i := range scanTasks {
		scanTasks[i].AddConsumer(orderTasks[i], DATA)
	}
	for i := range orderTasks {
		orderTasks[i].AddConsumer(mergeTask, DATA)
	}
	outOp := new(outputOp)
	outTask := NewTask(FILL, &wg, outOp.Exec)
	ws[0].AddTask(outTask)
	mergeTask.AddConsumer(outTask, DATA)
	go workers.Run()
	wg.Wait()
	workers.Stop()
}

func (o *testScanOp) Exec(msg *Message) ([]*Message, int, error) {
	if msg != nil && msg.IsStopMessage() {
		return nil, END, nil
	}
	if o.cnt == 4 {
		o.bat = nil
		return nil, END, nil
	}
	if o.bat == nil {
		o.bat = batch.New(1, fs)
		vec, _ := vector.New(vector.FLAT, types.New(types.T_int64), fs)
		o.bat.SetVector(0, vec)
	}
	vec := o.bat.GetVector(0)
	vec.Reset()
	vs := make([]int64, 8192)
	for i := range vs {
		vs[i] = int64(i + o.cnt)
	}
	vector.AppendList(vec, vs, nil)
	o.cnt++
	o.bat.SetRows(8192)
	msg = NewMessage(DATA, NEEDDUP, -1, o.bat)
	return []*Message{msg}, EVAL, nil
}

func (o *testOrderOp) Exec(msg *Message) ([]*Message, int, error) {
	if msg == nil {
		return nil, END, nil
	}
	vec := msg.bat.GetVector(0)
	vs := vector.GetColumnValue[int64](vec)
	sort.Slice(vs, func(i, j int) bool { return vs[i] < vs[j] })
	return []*Message{msg}, FILL, nil
}

func (o *testMergeOrderOp) Exec(msg *Message) ([]*Message, int, error) {
	if msg == nil && o.state == FILL {
		o.numProducer--
		if o.numProducer == 0 {
			o.state = EVAL
			vec := o.bat.GetVector(0)
			vs := vector.GetColumnValue[int64](vec)
			o.bat.SetRows(len(vs))
			sort.Slice(vs, func(i, j int) bool { return vs[i] < vs[j] })
			return nil, EVAL, nil
		}
		return nil, FILL, nil
	}
	switch o.state {
	case FILL:
		if o.bat == nil {
			o.bat = batch.New(1, fs)
			vec, _ := vector.New(vector.FLAT, types.New(types.T_int64), fs)
			o.bat.SetVector(0, vec)
		}
		if err := o.bat.GetVector(0).GetUnionFunction()(msg.bat.GetVector(0), nil); err != nil {
			return nil, END, nil
		}
		return nil, FILL, nil
	case EVAL:
		if o.obat == nil {
			o.obat = batch.New(1, fs)
			vec, _ := vector.New(vector.FLAT, types.New(types.T_int64), fs)
			o.obat.SetVector(0, vec)
			o.sels = make([]uint32, 1)
		}
		vec := o.obat.GetVector(0)
		vec.Reset()
		uf := vec.GetUnionFunction()
		for i := 0; i < 8192 && o.start < o.bat.Rows(); i++ {
			o.sels[0] = uint32(o.start)
			if err := uf(o.bat.GetVector(0), o.sels); err != nil {
				return nil, END, nil
			}
			o.start++
		}
		if o.start == o.bat.Rows() {
			o.state = END
		}
		o.obat.SetRows(vec.Length())
		msg := NewMessage(DATA, 0, -1, o.obat)
		return []*Message{msg}, o.state, nil
	default: // END
		return nil, END, nil
	}
}

func (o *outputOp) Exec(msg *Message) ([]*Message, int, error) {
	if msg == nil {
		return nil, END, nil
	}
	return nil, FILL, nil
}
