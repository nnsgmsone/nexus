package indextable

import "github.com/dolthub/maphash"

const (
	// MaxLoadFactor is the maximum load factor of the map
	MaxLoadFactor = 2

	MinMapSize = 1 << 10

	UnitLimit = 256
)

type Index struct {
	hashs  []uint64
	keys   [][]byte
	values []uint64
	tbl    *BytesTable
}

type Table[K comparable] struct {
	bits     uint64
	count    uint64
	mask     uint64
	maxCount uint64
	buckets  []bucket[K]
	hasher   maphash.Hasher[K]
}

type BytesTable struct {
	bits     uint64
	count    uint64
	mask     uint64
	maxCount uint64
	buckets  []bytesBucket
	hasher   maphash.Hasher[string]
}

type bucket[K comparable] struct {
	key   K
	index uint64
}

type bytesBucket struct {
	key   []byte
	index uint64
}
