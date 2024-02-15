package pipeline

import "sync"

var messagePool = &sync.Pool{
	New: func() any {
		return new(Message)
	},
}

var packetPool = &sync.Pool{
	New: func() any {
		return new(Packet)
	},
}

var ctrlPacketPool = &sync.Pool{
	New: func() any {
		return new(ctrlPacket)
	},
}
