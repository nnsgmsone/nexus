package pipeline

import (
	"sync"

	"github.com/google/uuid"
)

func NewTask(state int, wg *sync.WaitGroup,
	exec func(*Message) ([]*Message, int, error)) *Task {
	wg.Add(1)
	t := &Task{
		wg:    wg,
		exec:  exec,
		state: state,
		id:    uuid.New(),
	}
	return t
}

func (t *Task) AddConsumer(c *Task, typ int) int {
	if c.wid == t.wid {
		t.consumers = append(t.consumers, c)
	} else {
		t.consumers = append(t.consumers, &Task{
			id:  c.id,
			wid: c.wid,
		})
	}
	t.consumersType = append(t.consumersType, typ)
	return len(t.consumers) - 1
}

func (t *Task) produce(w *Worker) {
	if err := t.consume(t.msg, w); err != nil {
		w.err = err
		t.state = END
	}
	switch t.state {
	case FILL:
		w.addFillTask(t)
	case EVAL:
		w.addEvalTask(t)
	case END:
		if w.getTask(t.id) != nil {
			for i := range t.consumers { // clear all consumer
				if t.consumersType[i] == DATA {
					t.sendMessageToConsumer(nil, t.consumers[i], w)
				}
			}
			t.wg.Done()
			w.delTask(t)
		}
	}
	if t.msg != nil && t.msg.GetClass() == CTRL {
		t.releaseMessage()
	}
}

func (t *Task) consume(msg *Message, w *Worker) error {
	msgs, state, err := t.exec(msg)
	for i := range msgs {
		t.sendMessage(msgs[i], w)
	}
	t.state = state
	return err
}

func (t *Task) sendMessage(msg *Message, w *Worker) {
	if msg == nil {
		return
	}
	for i := range t.consumers {
		if msg.consumer >= 0 && msg.consumer != int32(i) {
			continue
		}
		if msg.consumer < 0 && t.consumersType[i] == CTRL {
			continue
		}
		t.sendMessageToConsumer(msg, t.consumers[i], w)
	}
}

func (t *Task) sendMessageToConsumer(msg *Message, c *Task, w *Worker) error {
	if c.wid == w.id {
		switch c.state {
		case FILL:
			c.msg = msg
			c.state = EVAL
			w.moveTaskToEval(c)
		case EVAL:
			pkt := &Packet{
				msg:    msg,
				taskID: c.id,
			}
			pkt.wg.Add(1)
			w.buffer = append(w.buffer, pkt)
		default:
			return nil
		}
		w.delEvalTask(c)
		c.produce(w)
		return nil
	}
	w.sendMessage(msg, c)
	return nil
}

func (t *Task) recvMessage(w *Worker, pkt *Packet) {
	switch t.state {
	case FILL:
		t.state = EVAL
		if pkt.msg == nil {
			t.releaseMessage()
			t.msg = nil
		} else {
			if pkt.hasTag(NEEDDUP) {
				if t.msg == nil || !t.msg.bat.SchemaEqual(pkt.msg.bat) {
					t.releaseMessage()
					t.msg = messagePool.Get().(*Message)
					t.msg.Reset(pkt.msg.class, EMPTYFLG,
						pkt.msg.consumer, pkt.msg.bat)
					t.msg.bat = pkt.msg.bat.Dup()
				} else {
					t.msg.bat.Reset()
					t.msg.bat.Append(pkt.msg.bat)
				}
			} else {
				t.releaseMessage()
				t.msg = messagePool.Get().(*Message)
				t.msg.Reset(pkt.msg.class, EMPTYFLG,
					pkt.msg.consumer, pkt.msg.bat)
			}
		}
		pkt.wg.Done()
		w.moveTaskToEval(t)
	case EVAL:
		if t.msg == nil {
			if pkt.msg != nil {
				t.releaseMessage()
				t.msg = messagePool.Get().(*Message)
				t.msg.Reset(pkt.msg.class, EMPTYFLG,
					pkt.msg.consumer, pkt.msg.bat)
				if pkt.hasTag(NEEDDUP) {
					t.msg.bat = pkt.msg.bat.Dup()
				}
			}
			pkt.wg.Done()
		} else {
			w.buffer = append(w.buffer, pkt)
		}
	case END:
		pkt.wg.Done()
		w.delTask(t)
	}
}

func (t *Task) releaseMessage() {
	if t.msg != nil {
		messagePool.Put(t.msg)
		t.msg = nil
	}
}
