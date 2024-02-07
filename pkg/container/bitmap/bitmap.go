package bitmap

import (
	"math/bits"

	"github.com/nnsgmsone/nexus/pkg/encoding"
	"github.com/nnsgmsone/nexus/pkg/vm/mheap"
)

func New(n int) (*Bitmap, error) {
	var bm Bitmap

	if n <= 0 {
		n = 1
	}
	// round up to 64-bit alignment
	data := mheap.Alloc(int64((n + 0x3F) & -0x40))
	bm.len = n
	bm.isEmpty = true
	bm.data = encoding.DecodeSlice[uint64](data)
	return &bm, nil
}

func (bm *Bitmap) Reset() {
	bm.isEmpty = true
	for i := range bm.data {
		bm.data[i] = 0
	}
}

// Size returns the size of the Bitmap
func (bm *Bitmap) Size() int {
	return len(bm.data) * 8
}

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false.
func (bm *Bitmap) IsEmpty() bool {
	if bm.isEmpty {
		return true
	}
	for i, j := 0, bm.len>>6; i < j; i++ {
		if bm.data[i] != 0 {
			return false
		}
	}
	if offset := bm.len & 0x3F; offset > 0 {
		start := (bm.len >> 6) << 6
		for i, j := start, start+offset; i < j; i++ {
			if bm.Contains(uint64(i)) {
				return false
			}
		}
	}
	bm.isEmpty = true
	return true
}

func (bm *Bitmap) Add(row uint64) error {
	if row >= uint64(bm.len) {
		if err := bm.expand(int(row + 1)); err != nil {
			return err
		}
	}
	bm.data[row>>6] |= 1 << (row & 0x3F)
	bm.isEmpty = false
	return nil
}

func (bm *Bitmap) Remove(row uint64) {
	if row < uint64(bm.len) {
		bm.data[row>>6] &^= (uint64(1) << (row & 0x3F))
	}
}

// Contains returns true if the row is contained in the Bitmap
func (bm *Bitmap) Contains(row uint64) bool {
	if row >= uint64(bm.len) {
		return false
	}
	return (bm.data[row>>6] & (1 << (row & 0x3F))) != 0
}

func (bm *Bitmap) Count() int {
	var cnt int

	for i, j := 0, bm.len>>6; i < j; i++ {
		cnt += bits.OnesCount64(bm.data[i])
	}
	if offset := bm.len & 0x3F; offset > 0 {
		start := (bm.len >> 6) << 6
		for i, j := start, start+offset; i < j; i++ {
			if bm.Contains(uint64(i)) {
				cnt++
			}
		}
	}
	bm.isEmpty = cnt == 0
	return cnt
}

func (bm *Bitmap) Shrink(sels []uint32) error {
	cnt := bm.Count()
	if cnt == 0 {
		return nil
	}
	if len(sels) == 0 {
		bm.Reset()
		return nil
	}
	bt, err := New(cnt)
	if err != nil {
		return err
	}
	for _, sel := range sels {
		if sel < 0 || sel >= uint32(cnt) {
			continue
		}
		if bm.Contains(uint64(sel)) {
			bt.Add(uint64(sel))
		}
	}
	bm.len = bt.len
	bm.data = bt.data
	return nil
}

func (bm *Bitmap) expand(n int) error {
	data := mheap.Alloc(int64((n + 0x3F) & -0x40))
	bm.len = n
	oldData := bm.data
	bm.data = encoding.DecodeSlice[uint64](data)
	copy(bm.data, oldData)
	return nil
}
