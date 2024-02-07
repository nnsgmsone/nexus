package noah

import (
	"bytes"

	"github.com/nnsgmsone/nexus/pkg/encoding"
)

func decodeLog(log []byte) logEntry {
	var l logEntry

	l.typ = encoding.Decode[uint64](log)
	l.logData = log[8:]
	return l
}

func encodeLog(l logEntry) []byte {
	var buf bytes.Buffer

	buf.Write(encoding.Encode(&l.typ))
	buf.Write(l.logData)
	return buf.Bytes()
}
