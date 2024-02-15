package pipeline

func (q *ctrlPacketQueue) push(ctrl *ctrlPacket) {
	q.Lock()
	defer q.Unlock()
	q.ctrls = append(q.ctrls, ctrl)
}

func (q *ctrlPacketQueue) pop() *ctrlPacket {
	q.Lock()
	defer q.Unlock()
	if len(q.ctrls) == 0 {
		return nil
	}
	ctrl := q.ctrls[0]
	q.ctrls = q.ctrls[1:]
	return ctrl
}
