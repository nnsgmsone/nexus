package indextable

import (
	"bytes"

	"github.com/dolthub/maphash"
	"github.com/nnsgmsone/nexus/pkg/encoding"
)

func NewBytesTable(cap int) *BytesTable {
	if cap < MinMapSize {
		cap = MinMapSize
	}
	t := &BytesTable{
		hasher: maphash.NewHasher[string](),
	}
	t.rehash(uint64(cap))
	return t
}

func (t *BytesTable) Count() uint64 {
	return t.count
}

func (t *BytesTable) Insert(hs []uint64, keys [][]byte, values []uint64) {
	if size := uint64(len(keys)) + t.count; size > t.maxCount {
		t.rehash(size)
	}
	t.hashKeys(hs, keys)
	for i := range keys {
		b := t.find(hs[i], keys[i])
		if b.index == 0 {
			t.count++
			b.key = keys[i]
			b.index = t.count
		}
		values[i] = b.index
	}
}

func (t *BytesTable) Lookup(hs []uint64, keys [][]byte, values []uint64) {
	t.hashKeys(hs, keys)
	for i := range keys {
		b := t.find(hs[i], keys[i])
		values[i] = b.index
	}
}

func (t *BytesTable) rehash(size uint64) {
	bits := t.bits + 2
	count := uint64(1) << bits
	maxCount := count / MaxLoadFactor
	for count < size {
		bits++
		count <<= 1
		maxCount = count / MaxLoadFactor
	}
	oldBuckets := t.buckets
	t.bits = bits
	t.mask = count - 1
	t.maxCount = maxCount
	t.buckets = make([]bytesBucket, count)
	for i := range oldBuckets {
		b := &oldBuckets[i]
		if b.index != 0 {
			nb := t.findEmpty(t.hasher.Hash(encoding.Bytes2String(b.key)))
			*nb = *b
		}
	}
}

func (t *BytesTable) find(h uint64, k []byte) *bytesBucket {
	for i := (h & t.mask); ; i = (i + 1) & t.mask {
		b := &t.buckets[i]
		if b.index == 0 || bytes.Equal(b.key, k) {
			return b
		}
	}
}

func (t *BytesTable) findEmpty(h uint64) *bytesBucket {
	for i := (h & t.mask); ; i = (i + 1) & t.mask {
		b := &t.buckets[i]
		if b.index == 0 {
			return b
		}
	}
}

func (t *BytesTable) hashKeys(hs []uint64, keys [][]byte) {
	for i, key := range keys {
		hs[i] = t.hasher.Hash(encoding.Bytes2String(key))
	}
}
