package plan

import (
	"errors"
	"fmt"

	"github.com/nnsgmsone/nexus/pkg/spl/parser"
	"github.com/nnsgmsone/nexus/pkg/spl/tree"
	"github.com/nnsgmsone/nexus/pkg/vfs"
	"github.com/nnsgmsone/nexus/pkg/vm/engine"
)

func New(spl string, fs vfs.FS, e engine.Engine) *Build {
	return &Build{e: e, fs: fs, spl: spl}
}

func (b *Build) Build() (*Plan, error) {
	stmt, err := parser.Parse(b.spl)
	if err != nil {
		return nil, err
	}
	return b.buildStatement(stmt)
}

func (b *Build) buildStatement(stmt *tree.Select) (*Plan, error) {
	root, err := b.buildCommandList(stmt.Cs)
	if err != nil {
		return nil, err
	}
	return &Plan{
		Root: Optimize(root),
	}, nil
}

func (b *Build) buildCommandList(cs []tree.CommandStatement) (*Scope, error) {
	var err error
	var in *Scope
	var out *Scope

	if len(cs) == 0 {
		return nil, errors.New("Usage: | command")
	}
	if importStmt, ok := cs[0].(*tree.Import); ok {
		return b.buildLoad(importStmt)
	}
	in, err = b.buildScanScope()
	if err != nil {
		return nil, err
	}
	if _, ok := cs[0].(*tree.Extract); !ok {
		return nil, errors.New("Usage: | extract command | command")
	}
	for i := 0; i < len(cs); i++ {
		if out, err = b.buildCommand(in, cs[i]); err != nil {
			return nil, err
		}
		in = out
	}
	return in, nil
}

func (b *Build) buildCommand(in *Scope, c tree.CommandStatement) (*Scope, error) {
	switch stmt := c.(type) {
	case *tree.Eval:
		return b.buildEvalScope(in, stmt)
	case *tree.Limit:
		return b.buildLimitScope(in, stmt)
	case *tree.Stats:
		return b.buildStatsScope(in, stmt)
	case *tree.Where:
		return b.buildWhereScope(in, stmt)
	case *tree.OrderBy:
		return b.buildOrderByScope(in, stmt)
	case *tree.Extract:
		return b.buildExtractScope(in, stmt)
	default:
		return nil, fmt.Errorf("unsupport command '%v'", c)
	}
}

func (b *Build) allocID() uint32 {
	id := b.id
	b.id++
	return id
}

func (b *Build) allocScope() *Scope {
	if len(b.scopes) == 0 {
		b.scopes = make([]Scope, 8)
	}
	s := &b.scopes[0]
	b.scopes = b.scopes[1:]
	return s
}

// ... -> in -> out -> ...
func (b *Build) newScope(in *Scope, typ int) *Scope {
	out := b.allocScope()
	in.Parent = out
	out.ScopeType = typ
	out.Children = append(out.Children, in)
	return out
}

func (b *Build) loadFile(path string) ([]byte, error) {
	return b.fs.ReadFile(path)
}

func newScope(in *Scope, typ int) *Scope {
	out := new(Scope)
	in.Parent = out
	out.ScopeType = typ
	out.Children = append(out.Children, in)
	return out
}
