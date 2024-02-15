package compile

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/nnsgmsone/nexus/pkg/container/batch"
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/spl/colexec/emit"
	"github.com/nnsgmsone/nexus/pkg/spl/colexec/exchange"
	"github.com/nnsgmsone/nexus/pkg/spl/colexec/filter"
	"github.com/nnsgmsone/nexus/pkg/spl/colexec/group"
	"github.com/nnsgmsone/nexus/pkg/spl/colexec/limit"
	"github.com/nnsgmsone/nexus/pkg/spl/colexec/load"
	"github.com/nnsgmsone/nexus/pkg/spl/colexec/projection"
	"github.com/nnsgmsone/nexus/pkg/spl/colexec/scan"
	"github.com/nnsgmsone/nexus/pkg/spl/colexec/top"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/spl/plan"
	"github.com/nnsgmsone/nexus/pkg/vm/engine"
	"github.com/nnsgmsone/nexus/pkg/vm/pipeline"
	"github.com/nnsgmsone/nexus/pkg/vm/process"
)

func New(spl string, e engine.Engine, proc *process.Process,
	emit func(*batch.Batch) error) (*Compile, error) {
	pn, err := plan.New(spl, proc.FS(), e).Build()
	if err != nil {
		return nil, err
	}
	return &Compile{
		e:    e,
		pn:   pn,
		spl:  spl,
		emit: emit,
		proc: proc,
		mcpu: runtime.NumCPU(),
		ws:   pipeline.NewWorkers(0),
	}, nil
}

func (c *Compile) Compile() error {
	c.ctx.signals = make(map[*plan.Scope][]*pipeline.Task)
	if err := c.compile(c.pn.Root); err != nil {
		return err
	}
	return c.compileEmit()
}

func (c *Compile) Run() error {
	var err error

	defer c.free()
	if err = c.specialize(); err != nil {
		return err
	}
	go func() {
		err = c.ws.Run()
	}()
	c.wg.Wait()
	c.ws.Stop()
	return err
}

func (c *Compile) Columns() []string {
	cols := make([]string, len(c.pn.Root.Attrs))
	for i := range cols {
		cols[i] = c.pn.Root.Attrs[i].Name
	}
	return cols
}

func (c *Compile) specialize() error {
	for _, op := range c.ops {
		if err := op.Specialize(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compile) free() {
	for _, op := range c.ops {
		op.Free()
	}
}

func (c *Compile) compile(s *plan.Scope) error {
	switch s.ScopeType {
	case plan.Scan_Scope:
		if err := c.compileScan(s); err != nil {
			c.free()
			return err
		}
	case plan.Import_Scope:
		if err := c.compileLoad(s); err != nil {
			c.free()
			return err
		}
	case plan.Limit_Scope:
		if err := c.compile(s.Children[0]); err != nil {
			c.free()
			return err
		}
		if err := c.compileLimit(s); err != nil {
			c.free()
			return err
		}
	case plan.Order_Scope:
		if err := c.compile(s.Children[0]); err != nil {
			c.free()
			return err
		}
		if err := c.compileOrder(s); err != nil {
			c.free()
			return err
		}
	case plan.Group_Scope:
		if err := c.compile(s.Children[0]); err != nil {
			c.free()
			return err
		}
		if err := c.compileGroup(s); err != nil {
			c.free()
			return err
		}
	case plan.Filter_Scope:
		if err := c.compile(s.Children[0]); err != nil {
			c.free()
			return err
		}
		if err := c.compileFilter(s); err != nil {
			c.free()
			return err
		}
	case plan.Projection_Scope:
		if err := c.compile(s.Children[0]); err != nil {
			c.free()
			return err
		}
		if err := c.compileProjection(s); err != nil {
			c.free()
			return err
		}
	default:
		return fmt.Errorf("unknown scope type: %v", s.ScopeType)
	}
	return nil
}

func (c *Compile) compileEmit() error {
	emitOps := make([]Operator, len(c.ctx.tasks))
	for i := range emitOps {
		emitOps[i] = emit.New(c.emit)
	}
	c.compileTasks(pipeline.FILL, emitOps)
	c.addSingalTask(nil)
	return nil
}

func (c *Compile) compileScan(s *plan.Scope) error {
	r, err := c.e.NewReader(c.proc)
	if err != nil {
		return err
	}
	if s.Scan.Extract == nil {
		return fmt.Errorf("scan extract is nil")
	}
	scanOp := scan.New(r, s.Scan.Extract.Lua, s.Scan.Extract.Cols, c.proc)
	c.compileTasks(pipeline.EVAL, []Operator{scanOp})
	c.registerSignal(s)
	return nil
}

func (c *Compile) compileLoad(s *plan.Scope) error {
	loadOp := load.New(s.Import.Paths, c.e, c.proc)
	c.compileTasks(pipeline.EVAL, []Operator{loadOp})
	return nil
}

func (c *Compile) compileFilter(s *plan.Scope) error {
	if s.Filter.Filter == nil {
		return nil
	}
	if typ := s.Filter.Filter.ResultType(); (&typ).Oid() != types.T_bool {
		return fmt.Errorf("filter must be boolean: %v", typ)
	}
	fn := func(mcpu int) []Operator {
		ops := make([]Operator, mcpu)
		for i := range ops {
			ops[i] = filter.New(s.Filter.Filter.Dup(), c.proc)
		}
		return ops
	}
	if len(c.ctx.tasks) == 1 {
		filterOps := fn(c.mcpu)
		c.compileExchangeTasks(pipeline.FILL, filterOps)
		return nil
	}
	filterOps := fn(len(c.ctx.tasks))
	c.compileTasks(pipeline.FILL, filterOps)
	return nil
}

func (c *Compile) compileProjection(s *plan.Scope) error {
	if len(s.Projection.ProjectionList) == 0 {
		return nil
	}
	fn := func(mcpu int) []Operator {
		ops := make([]Operator, mcpu)
		for i := range ops {
			exprs := make([]expr.Expr, len(s.Projection.ProjectionList))
			for j := range exprs {
				exprs[j] = s.Projection.ProjectionList[j].Dup()
			}
			ops[i] = projection.New(exprs, c.proc)
		}
		return ops
	}
	if len(c.ctx.tasks) == 1 {
		projectionOps := fn(c.mcpu)
		c.compileExchangeTasks(pipeline.FILL, projectionOps)
		return nil
	}
	projectOps := fn(len(c.ctx.tasks))
	c.compileTasks(pipeline.FILL, projectOps)
	return nil
}

func (c *Compile) compileLimit(s *plan.Scope) error {
	if s.Limit.Limit <= 0 {
		return fmt.Errorf("limit must be positive: %v", s.Limit.Limit)
	}
	if len(c.ctx.tasks) > 1 {
		limitOps := make([]Operator, len(c.ctx.tasks))
		for i := range limitOps {
			limitOps[i] = limit.New(uint64(s.Limit.Limit), 1)
		}
		c.compileTasks(pipeline.FILL, limitOps)
	}
	limitOp := limit.New(uint64(s.Limit.Limit), len(c.ctx.tasks))
	c.compileMergeTask(pipeline.FILL, limitOp)
	consumers := c.addSingalTask(s.Signals)
	limitOp.AddCtrlConsumer(consumers...)
	return nil
}

func (c *Compile) compileGroup(s *plan.Scope) error {
	var groupOps []Operator

	fn := func(mcpu int) []Operator {
		ops := make([]Operator, mcpu)
		for i := range ops {
			ops[i] = group.New(s.Group, false, c.proc, 1)
		}
		return ops
	}
	if len(c.ctx.tasks) == 1 {
		groupOps = fn(c.mcpu)
		c.compileExchangeTasks(pipeline.FILL, groupOps)
	} else {
		groupOps = fn(len(c.ctx.tasks))
		c.compileTasks(pipeline.FILL, groupOps)
	}
	groupOp := group.New(s.Group, true, c.proc, len(groupOps))
	c.compileMergeTask(pipeline.FILL, groupOp)
	return nil
}

func (c *Compile) compileOrder(s *plan.Scope) error {
	if s.Order.Limit <= 0 {
		return errors.New("order not support")
	}
	return c.compileTop(s)
}

func (c *Compile) compileTop(s *plan.Scope) error {
	var topOps []Operator

	fn := func(mcpu int) []Operator {
		ops := make([]Operator, mcpu)
		for i := range ops {
			ops[i] = top.New(s.Attrs, s.Order, c.proc, 1)
		}
		return ops
	}
	if len(c.ctx.tasks) == 1 {
		topOps := fn(c.mcpu)
		c.compileExchangeTasks(pipeline.FILL, topOps)
	} else {
		topOps := fn(len(c.ctx.tasks))
		c.compileTasks(pipeline.FILL, topOps)
	}
	topOp := top.New(s.Attrs, s.Order, c.proc, len(topOps))
	c.compileMergeTask(pipeline.FILL, topOp)
	return nil
}

func (c *Compile) compileTasks(state int, ops []Operator) {
	c.ops = append(c.ops, ops...)
	tasks := make([]*pipeline.Task, len(ops))
	if len(c.ctx.ws) == 0 {
		c.ctx.ws = make([]*pipeline.Worker, len(ops))
		for i := range c.ctx.ws {
			c.ctx.ws[i] = c.ws.AddWorker()
		}
	}
	for i, op := range ops {
		tasks[i] = pipeline.NewTask(state, &c.wg, op.GetExecFunc())
		c.ctx.ws[i].AddTask(tasks[i])
	}
	for i := range c.ctx.tasks {
		consumer := c.ctx.tasks[i].AddConsumer(tasks[i], pipeline.DATA)
		c.ctx.ops[i].AddMessageConsumer(int32(consumer))
	}
	c.ctx.ops = ops
	c.ctx.tasks = tasks
}

func (c *Compile) compileMergeTask(state int, op Operator) {
	if len(c.ctx.tasks) == 0 {
		return
	}
	c.ops = append(c.ops, op)
	task := pipeline.NewTask(state, &c.wg, op.GetExecFunc())
	w := c.ws.AddWorker()
	w.AddTask(task)
	for i := range c.ctx.tasks {
		consumer := c.ctx.tasks[i].AddConsumer(task, pipeline.DATA)
		c.ctx.ops[i].AddMessageConsumer(int32(consumer))
	}
	c.ctx.ops = []Operator{op}
	c.ctx.ws = []*pipeline.Worker{w}
	c.ctx.tasks = []*pipeline.Task{task}
}

func (c *Compile) compileExchangeTasks(state int, ops []Operator) {
	// producer -> exchange
	exchangeOp := exchange.New(exchange.RoundRobin)
	c.compileTasks(pipeline.FILL, []Operator{exchangeOp})
	// producer -> exchange -> consumers
	c.ops = append(c.ops, ops...)
	tasks := make([]*pipeline.Task, len(ops))
	ws := make([]*pipeline.Worker, len(ops))
	for i := range ws {
		ws[i] = c.ws.AddWorker()
	}
	for i, op := range ops {
		tasks[i] = pipeline.NewTask(state, &c.wg, op.GetExecFunc())
		ws[i].AddTask(tasks[i])
	}
	for i := range tasks {
		for j := range c.ctx.ops {
			consumer := c.ctx.tasks[j].AddConsumer(tasks[i], pipeline.DATA)
			c.ctx.ops[j].AddMessageConsumer(int32(consumer))
		}
	}
	c.ctx.ws = ws
	c.ctx.ops = ops
	c.ctx.tasks = tasks
}

func (c *Compile) addSingalTask(signals []*plan.Scope) []int32 {
	var consumers []int32

	task := c.ctx.tasks[0]
	if len(signals) == 0 {
		for _, v := range c.ctx.signals {
			for j := range v {
				consumers = append(consumers, int32(task.AddConsumer(v[j], pipeline.CTRL)))
			}
		}
	} else {
		for i := range signals {
			for j := range c.ctx.signals[signals[i]] {
				consumers = append(consumers, int32(task.AddConsumer(c.ctx.signals[signals[i]][j], pipeline.CTRL)))
			}
		}
	}
	return consumers
}

func (c *Compile) registerSignal(s *plan.Scope) {
	for i := range c.ctx.tasks {
		c.ctx.signals[s] = append(c.ctx.signals[s], c.ctx.tasks[i])
	}
}
