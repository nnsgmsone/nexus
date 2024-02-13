package plan

import (
	"go/constant"

	"github.com/nnsgmsone/nexus/pkg/spl/tree"
)

func (b *Build) buildLoad(loadStmt *tree.Import) (*Scope, error) {
	s := b.allocScope()
	s.ScopeType = Import_Scope
	s.Import = &Import{}
	s.Import.Paths = make([]string, len(loadStmt.Paths))
	for i, path := range loadStmt.Paths {
		s.Import.Paths[i] = constant.StringVal(path.Value)

	}
	return s, nil
}
