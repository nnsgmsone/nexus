package compare

import (
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"golang.org/x/exp/constraints"
)

func New(typ types.Type, asc bool) Compare {
	switch typ.Oid() {
	case types.T_bool:
		return newBoolCompare(asc)
	case types.T_int64:
		return newCompare[int64](asc)
	case types.T_float64:
		return newCompare[float64](asc)
	case types.T_string:
		return newStrCompare(asc)
	default:
		panic("not implemented")
	}
}

func newCompare[T constraints.Ordered](asc bool) *compare[T] {
	flg := 1
	if !asc {
		flg = -1
	}
	return &compare[T]{
		flg:  flg,
		vs:   make([][]T, 2),
		vecs: make([]*vector.Vector, 2),
	}
}

func (c *compare[T]) Set(i int, vec *vector.Vector) {
	c.vecs[i] = vec
	c.vs[i] = vector.GetColumnValue[T](vec)
}

func (c *compare[T]) Compare(veci, vecj int, vi, vj int64) int {
	var ret int

	ni, nj := c.vecs[veci].IsNull(int(vi)), c.vecs[vecj].IsNull(int(vj))
	// null first
	switch {
	case ni && !nj:
		ret = -1
	case !ni && nj:
		ret = 1
	}
	if ret != 0 {
		return ret * c.flg
	}
	if c.vs[veci][vi] < c.vs[vecj][vj] {
		ret = -1
	} else if c.vs[veci][vi] > c.vs[vecj][vj] {
		ret = 1
	}
	return ret * c.flg
}

// Copy copies the value from veci[vi] to vecj[vj].
func (c *compare[T]) Copy(veci, vecj int, vi, vj int64) error {
	if c.vecs[veci].IsNull(int(vi)) {
		c.vecs[vecj].SetNull(int(vj))
		return nil
	}
	c.vecs[vecj].SetNotNull(int(vj))
	c.vs[vecj][vj] = c.vs[veci][vi]
	return nil
}
