package tree

import "fmt"

type ExtractStatement interface {
	String() string
}

type Extract struct {
	Script ExtractScript
	Es     EvalExprList
}

type ExtractScript struct {
	Lua     *Value
	LuaFile *Value
}

func (e *Extract) String() string {
	if e.Script.Lua == nil {
		return fmt.Sprintf("extract lua_file = %s, %s", e.Script.LuaFile, e.Es)
	} else {
		return fmt.Sprintf("extract lua = %s, %s", e.Script.Lua, e.Es)
	}
}
