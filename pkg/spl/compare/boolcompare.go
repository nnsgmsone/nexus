package compare

import "github.com/nnsgmsone/nexus/pkg/container/vector"

func newBoolCompare(asc bool) *boolCompare {
	flg := 1
	if !asc {
		flg = -1
	}
	return &boolCompare{
		flg:  flg,
		vs:   make([][]bool, 2),
		vecs: make([]*vector.Vector, 2),
	}
}

func (c *boolCompare) Set(i int, vec *vector.Vector) {
	c.vecs[i] = vec
	c.vs[i] = vector.GetColumnValue[bool](vec)
}

func (c *boolCompare) Compare(veci, vecj int, vi, vj int64) int {
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
	if !c.vs[veci][vi] && c.vs[vecj][vj] {
		ret = -1
	} else if c.vs[veci][vi] && !c.vs[vecj][vj] {
		ret = 1
	}
	return ret * c.flg
}

// Copy copies the value from veci[vi] to vecj[vj].
func (c *boolCompare) Copy(veci, vecj int, vi, vj int64) error {
	if c.vecs[veci].IsNull(int(vi)) {
		c.vecs[vecj].SetNull(int(vj))
		return nil
	}
	c.vecs[vecj].SetNotNull(int(vj))
	c.vs[vecj][vj] = c.vs[veci][vi]
	return nil
}
