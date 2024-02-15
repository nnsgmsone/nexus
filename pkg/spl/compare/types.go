package compare

import (
	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/container/vector"
	"golang.org/x/exp/constraints"
)

type Compare interface {
	Set(int, *vector.Vector)
	Copy(int, int, int64, int64) error
	Compare(int, int, int64, int64) int
}

type compare[T constraints.Ordered] struct {
	flg  int
	vs   [][]T
	vecs []*vector.Vector
}

type strCompare struct {
	flg  int
	vs   [][]types.String
	vecs []*vector.Vector
}

type boolCompare struct {
	flg  int
	vs   [][]bool
	vecs []*vector.Vector
}
