package group

import (
	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/indextable"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/spl/colexec/agg"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/spl/plan"
	"github.com/nnsgmsone/nexus/pkg/util"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

func New(grp *plan.Group, needEval bool,
	proc *process.Process, producerNum int) *GroupOp {
	gs := make([]expr.Expr, len(grp.GroupBy))
	for i, e := range grp.GroupBy {
		gs[i] = e.Dup()
	}
	aggs := make([]Aggregate, len(grp.AggList))
	for i := range grp.AggList {
		es := make([]expr.Expr, len(grp.AggList[i].Es))
		typs := make([]types.Type, len(grp.AggList[i].Es))
		for j, e := range grp.AggList[i].Es {
			es[j] = e.Dup()
			typs[j] = e.ResultType()
		}
		aggs[i] = Aggregate{
			es:   es,
			args: typs,
			name: grp.AggList[i].FName,
			typ:  grp.AggList[i].Agg.ResultType(),
			agg:  agg.New(grp.AggList[i].FName, typs, grp.AggList[i].Agg.ResultType()),
		}
	}
	return &GroupOp{
		filled:      false,
		proc:        proc,
		aggs:        aggs,
		groupBy:     gs,
		needEval:    needEval,
		producerNum: producerNum,
	}
}

func (o *GroupOp) Specialize() error {
	for i := range o.groupBy {
		if err := o.groupBy[i].Specialize(o.proc); err != nil {
			return err
		}
	}
	o.aggVecs = make([][]*vector.Vector, len(o.aggs))
	for i := range o.aggs {
		for j := range o.aggs[i].es {
			if err := o.aggs[i].es[j].Specialize(o.proc); err != nil {
				return err
			}
		}
		o.aggVecs[i] = make([]*vector.Vector, len(o.aggs[i].es))
	}
	o.bat = batch.New(len(o.groupBy)+len(o.aggs), o.proc.FS())
	for i := range o.groupBy {
		typ := o.groupBy[i].ResultType()
		vec, err := vector.New(vector.FLAT, &typ, o.proc.FS())
		if err != nil {
			return err
		}
		o.bat.SetVector(i, vec)
	}
	for i := range o.aggs {
		if err := o.aggs[i].agg.Specialize(o.proc); err != nil {
			return err
		}
	}
	o.idx = indextable.NewIndex()
	o.bats = make([]*batch.Batch, 1)
	o.msgs = make([]*pipeline.Message, 1)
	o.grps = make([]uint32, indextable.UnitLimit)
	o.sels = make([]uint32, indextable.UnitLimit)
	o.ngrpsels = make([]uint32, indextable.UnitLimit)
	o.groupVecs = make([]*vector.Vector, len(o.groupBy))
	if len(o.groupBy) == 0 {
		for i := range o.aggs {
			o.aggs[i].agg.Grows(1)
		}
		if o.needEval {
			o.grps = o.grps[:0]
			o.sels = o.sels[:0]
			o.grps = append(o.grps, 0)
			o.sels = append(o.sels, 0)
		}
	}
	return nil
}

func (o *GroupOp) Exec(msg *pipeline.Message) ([]*pipeline.Message, int, error) {
	if msg == nil {
		o.producerNum--
		if o.producerNum > 0 {
			return nil, pipeline.FILL, nil
		}
		if !o.filled {
			return nil, pipeline.END, nil
		}
		if err := o.eval(); err != nil {
			return nil, pipeline.END, err
		}
		o.msgs[0] = util.NewDataMessage(o.bat)
		return o.msgs, pipeline.END, nil
	}
	o.bats[0] = msg.GetBatch()
	if o.bats[0].Rows() == 0 {
		return nil, pipeline.FILL, nil
	}
	o.filled = true
	if len(o.groupBy) == 0 {
		if o.needEval {
			if err := o.merge(o.bats[0], o.proc); err != nil {
				return nil, pipeline.END, err
			}
		} else {
			if err := o.process(o.bats[0], o.proc); err != nil {
				return nil, pipeline.END, err
			}
		}
		return nil, pipeline.FILL, nil
	}
	if o.needEval {
		if err := o.mergeWithGroup(o.bats[0], o.proc); err != nil {
			return nil, pipeline.END, err
		}
	} else {
		if err := o.processWithGroup(o.bats[0], o.proc); err != nil {
			return nil, pipeline.END, err
		}
	}
	return nil, pipeline.FILL, nil
}

func (o *GroupOp) GetExecFunc() func(*pipeline.Message) ([]*pipeline.Message, int, error) {
	return o.Exec
}

func (o *GroupOp) Free() error {
	return nil
}

func (o *GroupOp) eval() error {
	if o.needEval {
		for i := range o.aggs {
			vec, err := o.aggs[i].agg.Eval()
			if err != nil {
				return err
			}
			o.bat.SetVector(i+len(o.groupBy), vec)
		}
	} else {
		for i := range o.aggs {
			vec, err := o.aggs[i].agg.Save()
			if err != nil {
				return err
			}
			o.bat.SetVector(i+len(o.groupBy), vec)
		}
	}
	if len(o.groupBy) == 0 {
		o.bat.SetRows(1)
	} else {
		o.bat.SetRows(o.bat.GetVector(0).Length())
	}
	return nil
}

func (o *GroupOp) merge(bat *batch.Batch, proc *process.Process) error {
	for i := 0; i < bat.VectorCount(); i++ {
		agg := agg.New(o.aggs[i].name, o.aggs[i].args, o.aggs[i].typ)
		if err := agg.Specialize(proc); err != nil {
			return err
		}
		if err := agg.Load(bat.GetVector(i)); err != nil {
			return err
		}
		if err := o.aggs[i].agg.Merge(agg, o.grps, o.sels); err != nil {
			return err
		}
	}
	return nil
}

func (o *GroupOp) mergeWithGroup(bat *batch.Batch, proc *process.Process) error {
	rows := bat.Rows()
	for i := 0; i < len(o.groupVecs); i++ {
		o.groupVecs[i] = bat.GetVector(i)
	}
	for i := 0; i < rows; i += indextable.UnitLimit {
		n := rows - i
		if n > indextable.UnitLimit {
			n = indextable.UnitLimit
		}
		count := o.idx.Count()
		vals, err := o.idx.Insert(i, n, o.groupVecs)
		if err != nil {
			return err
		}
		if err := o.batchMerge(i, n, bat, count, vals); err != nil {
			return err
		}
	}
	return nil
}

func (o *GroupOp) process(bat *batch.Batch, proc *process.Process) error {
	if err := o.evalGroupByVector(); err != nil {
		return err
	}
	if err := o.evalAggVector(); err != nil {
		return err
	}
	for i := range o.aggs {
		if err := o.aggs[i].agg.BulkFill(0, o.aggVecs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (o *GroupOp) processWithGroup(bat *batch.Batch, proc *process.Process) error {
	if err := o.evalGroupByVector(); err != nil {
		return err
	}
	if err := o.evalAggVector(); err != nil {
		return err
	}
	rows := bat.Rows()
	for i := 0; i < rows; i += indextable.UnitLimit {
		n := rows - i
		if n > indextable.UnitLimit {
			n = indextable.UnitLimit
		}
		count := o.idx.Count()
		vals, err := o.idx.Insert(i, n, o.groupVecs)
		if err != nil {
			return err
		}
		if err := o.batchFill(i, n, bat, count, vals); err != nil {
			return err
		}
	}
	return nil
}

func (o *GroupOp) batchFill(start, count int, bat *batch.Batch,
	rows uint64, vals []uint64) error {
	cnt := 0
	o.grps = o.grps[:0]
	o.sels = o.sels[:0]
	o.ngrpsels = o.ngrpsels[:0]
	for k, v := range vals[:count] {
		if v > rows {
			rows++
			cnt++
			o.ngrpsels = append(o.ngrpsels, uint32(k+start))
		}
		o.grps = append(o.grps, uint32(v)-1)
		o.sels = append(o.sels, uint32(k+start))
	}
	if cnt > 0 {
		for j := 0; j < len(o.groupVecs); j++ {
			vec := o.bat.GetVector(j)
			if err := vec.GetUnionFunction()(o.groupVecs[j], o.ngrpsels); err != nil {
				return err
			}
		}
		for i := range o.aggs {
			o.aggs[i].agg.Grows(cnt)
		}
	}
	for i := range o.aggs {
		if err := o.aggs[i].agg.Fill(o.grps, o.sels, o.aggVecs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (o *GroupOp) batchMerge(start, count int, bat *batch.Batch,
	rows uint64, vals []uint64) error {
	cnt := 0
	o.grps = o.grps[:0]
	o.sels = o.sels[:0]
	o.ngrpsels = o.ngrpsels[:0]
	for k, v := range vals[:count] {
		if v > rows {
			rows++
			cnt++
			o.ngrpsels = append(o.ngrpsels, uint32(k+start))
		}
		o.grps = append(o.grps, uint32(v)-1)
		o.sels = append(o.sels, uint32(k+start))
	}
	if cnt > 0 {
		for j := 0; j < len(o.groupVecs); j++ {
			vec := o.bat.GetVector(j)
			if err := vec.GetUnionFunction()(o.groupVecs[j], o.ngrpsels); err != nil {
				return err
			}
		}
		for i := range o.aggs {
			o.aggs[i].agg.Grows(cnt)
		}
	}
	for i := range o.aggs {
		agg := agg.New(o.aggs[i].name, o.aggs[i].args, o.aggs[i].typ)
		if err := agg.Specialize(o.proc); err != nil {
			return err
		}
		if err := agg.Load(bat.GetVector(i + len(o.groupBy))); err != nil {
			return err
		}
		if err := o.aggs[i].agg.Merge(agg, o.grps, o.sels); err != nil {
			return err
		}
	}
	return nil
}

func (o *GroupOp) evalGroupByVector() error {
	for i := range o.groupBy {
		vec, err := o.groupBy[i].Eval(o.bats, o.proc)
		if err != nil {
			return err
		}
		o.groupVecs[i] = vec
	}
	return nil
}

func (o *GroupOp) evalAggVector() error {
	for i := range o.aggs {
		for j := range o.aggs[i].es {
			vec, err := o.aggs[i].es[j].Eval(o.bats, o.proc)
			if err != nil {
				return err
			}
			o.aggVecs[i][j] = vec
		}
	}
	return nil
}
