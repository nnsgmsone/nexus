package top

import (
	"container/heap"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/spl/compare"
	"github.com/nnsgmsone/nexus/pkg/spl/plan"
	"github.com/nnsgmsone/nexus/pkg/util"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

func New(attrs []plan.ScopeAttribute,
	order *plan.Order, proc *process.Process, producerNum int) *TopOp {
	return &TopOp{
		proc:        proc,
		attrs:       attrs,
		producerNum: producerNum,
		limit:       order.Limit,
		orders:      order.Orders,
	}
}

func (o *TopOp) Specialize() error {
	o.bats = make([]*batch.Batch, 1)
	o.sels = make([]uint32, 0, o.limit)
	o.msgs = make([]*pipeline.Message, 1)
	o.bat = batch.New(len(o.orders)+len(o.attrs), o.proc.FS())
	o.vecs = make([]*vector.Vector, len(o.orders)+len(o.attrs))
	o.cmps = make([]compare.Compare, len(o.orders)+len(o.attrs))
	for i, ord := range o.orders {
		typ := ord.E.ResultType()
		o.cmps[i] = compare.New(typ, ord.Type != plan.Descending)
		vec, err := vector.New(vector.FLAT, &typ, o.proc.FS())
		if err != nil {
			return err
		}
		o.bat.SetVector(i, vec)
	}
	for i, attr := range o.attrs {
		typ := attr.Type
		o.cmps[i+len(o.orders)] = compare.New(typ, true)
		vec, err := vector.New(vector.FLAT, &typ, o.proc.FS())
		if err != nil {
			return err
		}
		o.bat.SetVector(i+len(o.orders), vec)
	}
	return nil
}

func (o *TopOp) Exec(msg *pipeline.Message) ([]*pipeline.Message, int, error) {
	if o.limit <= 0 {
		return nil, pipeline.END, nil
	}
	if msg == nil {
		o.producerNum--
		if o.producerNum > 0 {
			return nil, pipeline.FILL, nil
		}
		if err := o.eval(); err != nil {
			return nil, pipeline.END, nil
		}
		o.msgs[0] = util.NewDataMessage(o.bat)
		return o.msgs, pipeline.END, nil
	}
	o.bats[0] = msg.GetBatch()
	rows := o.bats[0].Rows()
	if rows == 0 {
		return nil, pipeline.FILL, nil
	}
	if err := o.evalVector(); err != nil {
		return nil, pipeline.END, nil
	}
	if err := o.process(); err != nil {
		return nil, pipeline.END, nil
	}
	return nil, pipeline.FILL, nil
}

func (o *TopOp) GetExecFunc() func(*pipeline.Message) ([]*pipeline.Message, int, error) {
	return o.Exec
}

func (o *TopOp) Free() error {
	return nil
}

func (o *TopOp) process() error {
	var start int

	rows := o.bats[0].Rows()
	if n := len(o.sels); n < int(o.limit) {
		start = int(o.limit) - n
		if start > rows {
			start = rows
		}
		sels := make([]uint32, 1)
		for i := 0; i < start; i++ {
			for j := 0; j < o.bat.VectorCount(); j++ {
				vec := o.bat.GetVector(j)
				sels[0] = uint32(i)
				if err := vec.GetUnionFunction()(o.vecs[j], sels); err != nil {
					return err
				}
			}
			o.sels = append(o.sels, uint32(n))
			n++
		}
		if n == int(o.limit) {
			o.sort()
		}
	}
	if start == rows {
		return nil
	}
	for i, cmp := range o.cmps {
		cmp.Set(1, o.vecs[i])
	}
	for i, j := start, rows; i < j; i++ {
		if o.compare(1, 0, int64(i), int64(o.sels[0])) < 0 {
			for _, cmp := range o.cmps {
				if err := cmp.Copy(1, 0, int64(i), int64(o.sels[0])); err != nil {
					return err
				}
			}
			heap.Fix(o, 0)
		}
	}
	return nil
}

func (o *TopOp) eval() error {
	if len(o.sels) < int(o.limit) {
		o.sort()
	}
	sels := make([]uint32, len(o.sels))
	for i, j := 0, len(o.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(o).(uint32)
	}
	bat := o.bat
	o.bat = batch.New(len(o.attrs), o.proc.FS())
	for i, attr := range o.attrs {
		vec, err := vector.New(vector.FLAT, &attr.Type, o.proc.FS())
		if err != nil {
			return err
		}
		o.bat.SetVector(i, vec)
	}
	for i := 0; i < o.bat.VectorCount(); i++ {
		vec := o.bat.GetVector(i)
		if err := vec.GetUnionFunction()(bat.GetVector(i+len(o.orders)), sels); err != nil {
			return err
		}
	}
	o.bat.SetRows(len(sels))
	return nil
}

func (o *TopOp) sort() {
	for i, cmp := range o.cmps {
		cmp.Set(0, o.bat.GetVector(i))
	}
	heap.Init(o)
}

func (o *TopOp) evalVector() error {
	for i := range o.orders {
		vec, err := o.orders[i].E.Eval(o.bats, o.proc)
		if err != nil {
			return err
		}
		o.vecs[i] = vec
	}
	for i := 0; i < o.bats[0].VectorCount(); i++ {
		o.vecs[i+len(o.orders)] = o.bats[0].GetVector(i)
	}
	return nil
}
