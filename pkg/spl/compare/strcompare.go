package compare

import (
	"bytes"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
)

func newStrCompare(asc bool) *strCompare {
	flg := 1
	if !asc {
		flg = -1
	}
	return &strCompare{
		flg:  flg,
		vs:   make([][]types.String, 2),
		vecs: make([]*vector.Vector, 2),
	}
}

func (c *strCompare) Set(i int, vec *vector.Vector) {
	c.vecs[i] = vec
	c.vs[i] = vector.GetColumnValue[types.String](vec)
}

func (c *strCompare) Compare(veci, vecj int, vi, vj int64) int {
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
	vstri, err := c.vecs[veci].GetStringValue(c.vs[veci][vi])
	if err != nil {
		panic(err)
	}
	vstrj, err := c.vecs[vecj].GetStringValue(c.vs[vecj][vj])
	if err != nil {
		panic(err)
	}
	return bytes.Compare(vstri, vstrj) * c.flg
}

// Copy copies the value from veci[vi] to vecj[vj].
func (c *strCompare) Copy(veci, vecj int, vi, vj int64) error {
	if c.vecs[veci].IsNull(int(vi)) {
		c.vecs[vecj].SetNull(int(vj))
		return nil
	}
	c.vecs[vecj].SetNotNull(int(vj))
	str, err := c.vecs[veci].GetStringValue(c.vs[veci][vi])
	if err != nil {
		return err
	}
	return c.vecs[vecj].SetStringValue(c.vs[vecj][vj], str)
}
