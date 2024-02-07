package indextable

import (
	"github.com/dolthub/maphash"
)

func New[K comparable](cap int) *Table[K] {
	if cap < MinMapSize {
		cap = MinMapSize
	}
	t := &Table[K]{
		hasher: maphash.NewHasher[K](),
	}
	t.rehash(uint64(cap))
	return t
}

func (t *Table[K]) Count() uint64 {
	return t.count
}

func (t *Table[K]) Insert(hs []uint64, keys []K, values []uint64) {
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

func (t *Table[K]) Lookup(hs []uint64, keys []K, values []uint64) {
	t.hashKeys(hs, keys)
	for i := range keys {
		b := t.find(hs[i], keys[i])
		values[i] = b.index
	}
}

func (t *Table[K]) rehash(size uint64) {
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
	t.buckets = make([]bucket[K], count)
	for i := range oldBuckets {
		b := &oldBuckets[i]
		if b.index != 0 {
			nb := t.findEmpty(t.hasher.Hash(b.key))
			*nb = *b
		}
	}
}

func (t *Table[K]) find(h uint64, k K) *bucket[K] {
	for i := (h & t.mask); ; i = (i + 1) & t.mask {
		b := &t.buckets[i]
		if b.index == 0 || b.key == k {
			return b
		}
	}
}

func (t *Table[K]) findEmpty(h uint64) *bucket[K] {
	for i := (h & t.mask); ; i = (i + 1) & t.mask {
		b := &t.buckets[i]
		if b.index == 0 {
			return b
		}
	}
}

func (t *Table[K]) hashKeys(hs []uint64, keys []K) {
	for i, key := range keys {
		hs[i] = t.hasher.Hash(key)
	}
}
