package indextable

import (
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"github.com/nnsgmsone/nexus/pkg/encoding"
)

func NewIndex() *Index {
	return &Index{
		keys:   make([][]byte, UnitLimit),
		hashs:  make([]uint64, UnitLimit),
		values: make([]uint64, UnitLimit),
		tbl:    NewBytesTable(0),
	}
}

func (idx *Index) Count() uint64 {
	return idx.tbl.Count()
}

func (idx *Index) Insert(start, count int, vecs []*vector.Vector) ([]uint64, error) {
	var err error

	defer func() {
		for i := 0; i < count; i++ {
			idx.keys[i] = idx.keys[i][:0]
		}
	}()
	if err = idx.encodeKeys(vecs, start, count); err != nil {
		return nil, err
	}
	idx.tbl.Insert(idx.hashs[:count], idx.keys[:count], idx.values[:count])
	return idx.values[:count], nil
}

func (idx *Index) encodeKeys(vecs []*vector.Vector, start, count int) error {
	for _, vec := range vecs {
		if vec.GetType().IsFixedLen() {
			idx.encodeValues(vec, start, count)
		} else {
			if err := idx.encodeStrValues(vec, start, count); err != nil {
				return err
			}
		}
	}
	return nil
}

func (idx *Index) encodeValues(vec *vector.Vector, start, count int) {
	typ := vec.GetType()
	switch typ.Oid() {
	case types.T_bool:
		encodeValues[bool](idx, start, count, vec)
	case types.T_int64:
		encodeValues[int64](idx, start, count, vec)
	case types.T_float64:
		encodeValues[float64](idx, start, count, vec)
	}
}

func (idx *Index) encodeStrValues(vec *vector.Vector, start, count int) error {
	strs := vector.GetColumnValue[types.String](vec)
	for i := 0; i < count; i++ {
		if vec.IsNull(i + start) {
			idx.keys[i] = append(idx.keys[i], 1)
		} else {
			idx.keys[i] = append(idx.keys[i], 0)
			str, err := vec.GetStringValue(strs[i+start])
			if err != nil {
				return err
			}
			length := uint64(len(str))
			idx.keys[i] = append(idx.keys[i], encoding.Encode(&length)...)
			idx.keys[i] = append(idx.keys[i], str...)
		}
	}
	return nil
}

func encodeValues[T any](idx *Index, start, count int, vec *vector.Vector) {
	vs := vector.GetColumnValue[T](vec)
	for i := 0; i < count; i++ {
		if vec.IsNull(i + start) {
			idx.keys[i] = append(idx.keys[i], 1)
		} else {
			idx.keys[i] = append(idx.keys[i], 0)
			idx.keys[i] = append(idx.keys[i], encoding.Encode(&vs[i+start])...)
		}
	}
}
