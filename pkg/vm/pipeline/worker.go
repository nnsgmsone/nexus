package pipeline

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

func NewWorkers(num int) *Workers {
	ws := new(Workers)
	ws.ws = make([]*Worker, num)
	ws.mp = make(map[uuid.UUID]*Worker)
	ws.ctx, ws.cancel = context.WithCancel(context.Background())
	for i := range ws.ws {
		ctx, cancel := context.WithCancel(ws.ctx)
		ws.ws[i] = &Worker{
			ws:        ws,
			ctx:       ctx,
			cancel:    cancel,
			id:        uuid.New(),
			ch:        make(chan *Packet),
			ctrlCh:    make(chan *Packet),
			tasks:     make(map[uuid.UUID]*Task),
			evalTasks: make(map[uuid.UUID]*Task),
			fillTasks: make(map[uuid.UUID]*Task),
			ctrlQueue: new(ctrlPacketQueue),
			ctrlStop:  make(chan struct{}),
		}
		ws.ws[i].ctrlCond.L = &ws.ws[i].ctrlLock
		ws.mp[ws.ws[i].id] = ws.ws[i]
	}
	return ws
}

func (ws *Workers) AddWorker() *Worker {
	ctx, cancel := context.WithCancel(ws.ctx)
	w := &Worker{
		ws:        ws,
		ctx:       ctx,
		cancel:    cancel,
		id:        uuid.New(),
		ch:        make(chan *Packet),
		tasks:     make(map[uuid.UUID]*Task),
		evalTasks: make(map[uuid.UUID]*Task),
		fillTasks: make(map[uuid.UUID]*Task),
		ctrlCh:    make(chan *Packet),
		ctrlQueue: new(ctrlPacketQueue),
		ctrlStop:  make(chan struct{}),
	}
	w.ctrlCond.L = &w.ctrlLock
	ws.ws = append(ws.ws, w)
	ws.mp[w.id] = w
	return w
}

func (ws *Workers) Run() error {
	var err error
	var wg sync.WaitGroup

	defer ws.cancel()
	for i := range ws.ws {
		wg.Add(1)
		go func(idx int) {
			ws.ws[idx].run()
			wg.Done()
		}(i)
	}
	wg.Wait()
	for _, w := range ws.ws {
		if w.err != nil {
			err = w.err
			break
		}
	}
	return err
}

func (ws *Workers) Stop() {
	ws.cancel()
	for _, w := range ws.ws {
		w.stop()
	}
}

func (ws *Workers) GetWorkers() []*Worker {
	return ws.ws
}

func (ws *Workers) getWorker(id uuid.UUID) *Worker {
	ws.RLock()
	defer ws.RUnlock()
	return ws.mp[id]
}

func (w *Worker) AddTask(t *Task) {
	t.wid = w.id
	w.addTask(t.id, t)
	switch t.state {
	case FILL:
		w.addFillTask(t)
	case EVAL:
		w.addEvalTask(t)
	}
}

func (w *Worker) run() {
	w.wg.Add(1)
	go w.sendLoop()
	defer w.cancel()
	defer w.wg.Done()
	for {
		w.recvCtrl()
		if w.evalTaskNumber() == 0 {
			if w.recvMessage() {
				return
			}
		}
		t := w.popEvalTask() // the task is end
		if t == nil {
			continue
		}
		t.produce(w)
	}
}

func (w *Worker) cleanup() {
	for i := range w.buffer {
		w.buffer[i].wg.Done()
	}
	w.buffer = nil
}

func (w *Worker) stop() {
	w.wg.Wait()
	for _, t := range w.tasks {
		t.wg.Done()
	}
	for {
		select {
		case <-w.ctrlStop:
			return
		default:
			w.ctrlLock.Lock()
			w.ctrlCond.Signal()
			w.ctrlLock.Unlock()
		}
	}
}

func (w *Worker) sendLoop() {
	for {
		if w.send() {
			w.ctrlStop <- struct{}{}
			return
		}
	}
}

func (w *Worker) send() bool {
	var ctrl *ctrlPacket

	w.ctrlLock.Lock()
	for ctrl = w.ctrlQueue.pop(); ctrl == nil; ctrl = w.ctrlQueue.pop() {
		w.ctrlCond.Wait()
		select {
		case <-w.ctx.Done():
			w.ctrlLock.Unlock()
			return true
		default:
		}
	}
	w.ctrlLock.Unlock()
	ctrl.pkt.wg.Add(1)
	select {
	case <-ctrl.dest.ctx.Done():
		ctrl.pkt.wg.Done()
	case ctrl.dest.ctrlCh <- ctrl.pkt:
		ctrl.pkt.wg.Wait()
	}
	packetPool.Put(ctrl.pkt)
	ctrlPacketPool.Put(ctrl)
	return false
}

func (w *Worker) sendMessage(msg *Message, c *Task) {
	dest := w.ws.getWorker(c.wid)
	switch {
	case dest == nil: // cross-node
		panic("not implement")
	default:
		pkt := packetPool.Get().(*Packet)
		pkt.msg = msg
		pkt.taskID = c.id
		if msg != nil && msg.GetClass() == 1 {
			ctrlPkt := ctrlPacketPool.Get().(*ctrlPacket)
			ctrlPkt.pkt = pkt
			ctrlPkt.dest = dest
			w.ctrlQueue.push(ctrlPkt)
			w.ctrlLock.Lock()
			w.ctrlCond.Signal()
			w.ctrlLock.Unlock()
			return
		}
		pkt.wg.Add(1)
		select {
		case <-dest.ctx.Done():
			pkt.wg.Done()
			packetPool.Put(pkt)
		case dest.ch <- pkt:
			pkt.wg.Wait()
			packetPool.Put(pkt)
		}
	}
}

func (w *Worker) recvMessage() bool {
	var pkt *Packet

	if len(w.buffer) > 0 {
		pkt = w.buffer[0]
		w.buffer = w.buffer[1:]
	} else {
		select {
		case <-w.ctx.Done():
			return true
		case pkt = <-w.ch:
		}
	}
	t := w.getTask(pkt.taskID)
	if t == nil {
		pkt.wg.Done()
		return false
	}
	t.recvMessage(w, pkt)
	return false
}

func (w *Worker) recvCtrl() {
	select {
	case <-w.ctx.Done():
		return
	case pkt := <-w.ctrlCh:
		t := w.getTask(pkt.taskID)
		if t == nil {
			pkt.wg.Done()
			return
		}
		t.recvMessage(w, pkt)
	default:
	}
}

func (w *Worker) evalTaskNumber() int {
	return len(w.evalTasks)
}

func (w *Worker) popEvalTask() *Task {
	for k, v := range w.evalTasks {
		delete(w.evalTasks, k)
		return v
	}
	return nil
}

func (w *Worker) addEvalTask(t *Task) {
	w.evalTasks[t.id] = t
}

func (w *Worker) delEvalTask(t *Task) {
	delete(w.evalTasks, t.id)
}

func (w *Worker) addFillTask(t *Task) {
	w.fillTasks[t.id] = t
}

func (w *Worker) moveTaskToEval(t *Task) {
	w.evalTasks[t.id] = t
	delete(w.fillTasks, t.id)
}

func (w *Worker) addTask(id uuid.UUID, t *Task) {
	w.tasks[id] = t
}

func (w *Worker) getTask(id uuid.UUID) *Task {
	return w.tasks[id]
}

func (w *Worker) delTask(t *Task) {
	delete(w.tasks, t.id)
}
