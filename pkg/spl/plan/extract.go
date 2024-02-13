package plan

import (
	"fmt"
	"go/constant"

	"github.com/nnsgmsone/nexus/pkg/container/types"
	"github.com/nnsgmsone/nexus/pkg/spl/expr"
	"github.com/nnsgmsone/nexus/pkg/spl/tree"
)

func (b *Build) buildExtractScope(in *Scope, stmt *tree.Extract) (*Scope, error) {
	out := b.newScope(in, Extract_Scope)
	if len(in.Attrs) == 0 {
		return nil, fmt.Errorf("input scope must be have a schema")
	}
	out.Extract = new(Extract)
	name := tree.Name(in.Attrs[0].Name)
	e, err := b.buildExpr(in, tree.ColumnName{Path: name})
	if err != nil {
		return nil, err
	}
	out.Extract.E = e
	switch {
	case stmt.Script.Lua != nil:
		out.Extract.Lua = constant.StringVal(stmt.Script.Lua.Value)
	case stmt.Script.LuaFile != nil:
		luaFile := constant.StringVal(stmt.Script.LuaFile.Value)
		data, err := b.loadFile(luaFile)
		if err != nil {
			return nil, err
		}
		out.Extract.Lua = string(data)
	}
	out.Extract.Cols = make([]int, len(stmt.Es))
	for i := range stmt.Es {
		c, err := b.buildExpr(in, stmt.Es[i].E)
		if err != nil {
			return nil, err
		}
		as := string(stmt.Es[i].As)
		if len(as) == 0 {
			return nil, fmt.Errorf("unsupport extract statement %s", stmt)
		}
		if ce, ok := c.(*expr.ConstExpr); ok {
			cv := ce.ConstValue()
			if cv.Kind() != constant.Int {
				return nil, fmt.Errorf("unsupport extract statement %s", stmt)
			}
			off, ok := constant.Int64Val(cv)
			if !ok {
				return nil, fmt.Errorf("unsupport extract statement %s", stmt)
			}
			out.Extract.Cols[i] = int(off)
		}
		out.Attrs = append(out.Attrs, ScopeAttribute{
			Name: as,
			Type: types.StringType,
			ID:   b.allocID(),
		})
	}
	return out, nil
}
