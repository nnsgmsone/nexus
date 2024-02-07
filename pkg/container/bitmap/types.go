package bitmap

import (
	"bytes"
	"math/bits"
	"strconv"

	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vm/mheap"
)

type Bitmap struct {
	isEmpty bool
	// len represent the size of bitmap
	len  int
	data []uint64
}

func (bm *Bitmap) Marshal() ([]byte, error) {
	var buf bytes.Buffer

	length := uint64(bm.len)
	buf.Write(encoding.Encode(&length))
	length = uint64(len(bm.data) * 8)
	buf.Write(encoding.Encode(&length))
	buf.Write(encoding.EncodeSlice(bm.data))
	return buf.Bytes(), nil
}

func (bm *Bitmap) Unmarshal(data []byte) error {
	bm.isEmpty = false
	bm.len = int(encoding.Decode[uint64](data[:8]))
	data = data[8:]
	size := int(encoding.Decode[uint64](data[:8]))
	data = data[8:]
	if cap(bm.data) >= len(data[:size])/8 {
		bm.data = bm.data[:len(data[:size])/8]
		copy(bm.data, encoding.DecodeSlice[uint64](data[:size]))
	} else {
		newData := mheap.Alloc(int64(len(data[:size])))
		copy(newData, data[:size])
		bm.data = encoding.DecodeSlice[uint64](newData)
	}
	return nil
}

func (bm *Bitmap) String() string {
	str := "["
	start := uint64(0)
	for i := 0; i < len(bm.data); i++ {
		bit := bm.data[i]
		// loop over bits in the word
		for bit != 0 {
			t := bit & -bit
			if len(str) > 1 {
				str += ","
			}
			str += strconv.FormatUint(start+uint64(bits.OnesCount64(t-1)), 10)
			bit ^= t
		}
		start += 64
	}
	str += "]"
	return str
}
