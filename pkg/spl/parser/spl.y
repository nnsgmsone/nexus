%{

package parser

import (
	"go/constant"
	"github.com/nnsgmsone/nexus/pkg/spl/tree"
)

%}

%{

type splSymUnion struct {
    val any
    ss *tree.Select
}

func (u *splSymUnion) SelectStatement() *tree.Select {
    if u.ss == nil{
        u.ss = new(tree.Select)
    }
    return u.ss
}

func (u *splSymUnion) LimitStatement() *tree.Limit {
    return u.val.(*tree.Limit)
}

func (u *splSymUnion) OrderStatement() *tree.OrderBy {
    return u.val.(*tree.OrderBy)
}

func (u *splSymUnion) StatsStatement() *tree.Stats {
    return u.val.(*tree.Stats)
}

func (u *splSymUnion) WhereStatement() *tree.Where {
    return u.val.(*tree.Where)
}

func (u *splSymUnion) EvalStatement() *tree.Eval {
    return u.val.(*tree.Eval)
}

func (u *splSymUnion) ExtractStatement() *tree.Extract {
    return u.val.(*tree.Extract)
}

func (u *splSymUnion) ImportStatement() *tree.Import {
    return u.val.(*tree.Import)
}

func (u *splSymUnion) extractStatement() tree.ExtractStatement {
    return u.val.(tree.ExtractStatement)
}

func (u *splSymUnion) extractScriptOpt() tree.ExtractScript {
    return u.val.(tree.ExtractScript)
}

func (u *splSymUnion) evalExprStatement() tree.EvalExpr{
    return u.val.(tree.EvalExpr)
}

func (u *splSymUnion) evalExprListStatement() tree.EvalExprList{
    return u.val.(tree.EvalExprList)
}

func (u *splSymUnion) exprStatement() tree.ExprStatement {
    return u.val.(tree.ExprStatement)
}

func (u *splSymUnion) exprStatements() tree.ExprStatements {
    return u.val.(tree.ExprStatements)
}

func (u *splSymUnion) valueStatement() *tree.Value {
    if u.val == nil{
    	return nil
    }
    return u.val.(*tree.Value)
}

func (u *splSymUnion) valueStatements() []*tree.Value {
    return u.val.([]*tree.Value)
}


func (u *splSymUnion) statStatement() tree.Stat {
    return u.val.(tree.Stat)
}

func (u *splSymUnion) statListStatement() tree.StatList {
    return u.val.(tree.StatList)
}

func (u *splSymUnion) funcStatement() *tree.FuncExpr {
    return u.val.(*tree.FuncExpr)
}

func (u *splSymUnion) orderListStatement() tree.OrderList {
    return u.val.(tree.OrderList)
}

func (u *splSymUnion) orderStatement() *tree.Order{
    return u.val.(*tree.Order)
}

func (u *splSymUnion) direction() tree.Direction{
    return u.val.(tree.Direction)
}

func (u *splSymUnion) tableName() *tree.TableName{
    return u.val.(*tree.TableName)
}

func (u *splSymUnion) columnName() tree.ColumnName{
    return u.val.(tree.ColumnName)
}

func (u *splSymUnion) columnNameList() tree.ColumnNameList {
    return u.val.(tree.ColumnNameList)
}

func (u *splSymUnion) nameList() tree.NameList {
    return u.val.(tree.NameList)
}

func (u *splSymUnion) name() tree.Name {
    return u.val.(tree.Name)
}

func (u *splSymUnion) setNegative() *tree.Value{
    v, ok := u.val.(*tree.Value)
    if !ok {
        return nil
    }
    iv, _ := constant.Int64Val(v.Value)
    v.Value = constant.MakeInt64(-1 * iv)
    return v
}

%}

%token <str> IDENT
%token <union> ICONST FCONST SCONST
%token <str> LESS_EQUALS GREATER_EQUALS NOT_EQUALS

%token <str> AND AS ASC

%token <str> BOOL BY

%token <str> CAST

%token <str> DESC DEDUP
%token <str> DOUBLE

%token <str> EVAL

%token <str> FALSE
%token <str> FLOAT

%token <str> INT
%token <str> IMPORT

%token <str> LIMIT
%token <str> LONG
%token <str> LUA
%token <str> LUA_FILE

%token <str> NOT

%token <str> OR
%token <str> ORDER

%token <str> EXTRACT

%token <str> STATS
%token <str> STRING

%token <str> TRUE
%token <str> TYPE

%token <str> WHERE

%union {
    id      int32
    pos     int32
    byt     byte
    str     string
    union   splSymUnion
}

%type <union> stmt_block
%type <union> stmt

%type <union> select_stmt

%type <union> eval_clause limit_clause order_clause
              stats_clause where_clause dedup_clause extract_clause

%type <union> opt_extract_script

%type <union> import_clause

%type <union> opt_asc_desc

%type <union> eval_list eval_elem 

%type <str> name
%type <str> func_name

%type <union> column_name column_list

%type <union> name_list

%type <union> order_list
%type <union> expr_list

%type <union> a_expr b_expr c_expr d_expr
%type <union> order

%type <union> target_list target_elem
%type <union> typename

%type <union> cast_target

%type <union> signed_iconst

%type <union> func_application func_expr_common_subexpr
%type <union> func_expr

%type <byt> '+' '-' '*' '/' '%' '<' '>' '=' '(' ')' '|' ':'

%left      OR
%left      AND
%right     NOT
%nonassoc  '<' '>' '=' LESS_EQUALS GREATER_EQUALS NOT_EQUALS

%nonassoc  IDENT
%left      '+' '-'
%left      '*' '/' '%'

%left      '(' ')'

%%

stmt_block: '|' stmt
            {
                spllex.(*lexer).SetStmt($2.SelectStatement())
            }

stmt: select_stmt
      {
        $$.val = $1.SelectStatement()
      }
    | stmt '|' select_stmt
      {
        as, bs := $1.SelectStatement(), $3.SelectStatement()
        as.Cs = append(as.Cs, bs.Cs...)
        $$.val = as
      }

select_stmt: extract_clause
	     {
		ss := $$.SelectStatement()
		ss.Cs = append(ss.Cs, $1.ExtractStatement())
	     }
           | eval_clause
             {
                ss := $$.SelectStatement()
                ss.Cs = append(ss.Cs, $1.EvalStatement())
             }
           | limit_clause
             {
                ss := $$.SelectStatement()
                ss.Cs = append(ss.Cs, $1.LimitStatement())
             }
           | order_clause
             {
                ss := $$.SelectStatement()
                ss.Cs = append(ss.Cs, $1.OrderStatement())
             }
           | stats_clause
             {
                ss := $$.SelectStatement()
                ss.Cs = append(ss.Cs, $1.StatsStatement())
             }
           | where_clause
             {
                ss := $$.SelectStatement()
                ss.Cs = append(ss.Cs, $1.WhereStatement())
             }
           | dedup_clause
             {
                ss := $$.SelectStatement()
                ss.Cs = append(ss.Cs, $1.StatsStatement())
             }
	   | import_clause
	     {
	     	ss := $$.SelectStatement()
		ss.Cs = append(ss.Cs, $1.ImportStatement())
	     }

extract_clause: EXTRACT opt_extract_script eval_list 
	      	{
	     		$$.val = &tree.Extract{Script: $2.extractScriptOpt(), Es: $3.evalExprListStatement()}
		}

opt_extract_script: LUA '=' SCONST
	     {
	     	$$.val = tree.ExtractScript{Lua: $3.valueStatement()}
	     }
	   | LUA_FILE '=' SCONST
	     {
	     	$$.val = tree.ExtractScript{LuaFile: $3.valueStatement()}
	     }
	   

eval_clause: EVAL eval_list
             {
                $$.val = &tree.Eval{ $2.evalExprListStatement() }
             }

where_clause: WHERE a_expr
              {
                $$.val = &tree.Where{ $2.exprStatement() }
              }

limit_clause: LIMIT signed_iconst
              {
                $$.val = &tree.Limit{Count: $2.valueStatement() }
              }

dedup_clause: DEDUP column_list
              {
                $$.val = &tree.Stats{By: $2.columnNameList() }
              }


stats_clause: STATS target_list
              {
                $$.val = &tree.Stats{Ss: $2.statListStatement() }
              }
            | STATS target_list BY column_list
              {
                $$.val = &tree.Stats{Ss: $2.statListStatement(), By: $4.columnNameList() }
              }

import_clause: IMPORT name_list
	     {
	     	$$.val = &tree.Import{Paths: $2.valueStatements()}
	     }

name_list: SCONST
	   {
	   	$$.val = []*tree.Value{$1.valueStatement()}
	   }
	 | name_list ',' SCONST
	   {
	   	$$.val = append($1.valueStatements(), $3.valueStatement())
	   }

eval_list: eval_elem
           {
            $$.val = tree.EvalExprList{$1.evalExprStatement() }
           }
         | eval_list ',' eval_elem
           {
            $$.val = append($1.evalExprListStatement(), $3.evalExprStatement())
           }

eval_elem: name '=' a_expr
           {
            $$.val = tree.EvalExpr{ As: tree.Name($1), E: $3.exprStatement() }
           }

target_list: target_elem
             {
                $$.val = tree.StatList{ $1.statStatement() }
             }
           | target_list ',' target_elem
             {
                $$.val = append($1.statListStatement(), $3.statStatement())
             }

target_elem: func_name '(' expr_list ')' AS name
             {
                $$.val = tree.Stat{F: tree.Name($1), Es: $3.exprStatements(), As: tree.Name($6) }
             }
           | func_name '(' ')' AS name
             {
                $$.val = tree.Stat{F: tree.Name($1), As: tree.Name($5) }
             }
           | func_name '(' ')'
             {
                $$.val = tree.Stat{F: tree.Name($1) }
             }
           | func_name '(' expr_list ')'
             {
                $$.val = tree.Stat{F: tree.Name($1), Es: $3.exprStatements() }
             }

order_clause: ORDER BY order_list
              {
                $$.val = &tree.OrderBy{Orders: $3.orderListStatement(), Limit: nil }
              }
            | ORDER signed_iconst BY order_list
              {
                $$.val = &tree.OrderBy{Limit: $2.valueStatement(), Orders: $4.orderListStatement() }
              }

order_list: order
            {
                $$.val = tree.OrderList{$1.orderStatement()}
            }
          | order_list ',' order
            {
                $$.val = append($1.orderListStatement(), $3.orderStatement())
            }

order: column_name opt_asc_desc
       {
            $$.val = &tree.Order{
                E:          $1.exprStatement(),
                Type:       $2.direction(),
            }
       }

opt_asc_desc: ASC   { $$.val = tree.Ascending }
            | DESC  { $$.val = tree.Descending }
            |       { $$.val = tree.DefaultDirection }



expr_list: a_expr               { $$.val = tree.ExprStatements{$1.exprStatement()} }
         | expr_list ',' a_expr { $$.val = append($1.exprStatements(), $3.exprStatement()) }

a_expr: c_expr                  { $$.val = $1.exprStatement() }
      | NOT a_expr              { $$.val = &tree.FuncExpr{Name: "not", Args: tree.ExprStatements{$2.exprStatement()} }}
      | a_expr OR a_expr        { $$.val = &tree.FuncExpr{Name: "or", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | a_expr AND a_expr       { $$.val = &tree.FuncExpr{Name: "and", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | b_expr                  { $$.val = $1.exprStatement() }

b_expr: d_expr                  { $$.val = $1.exprStatement() }
      | column_name             { $$.val = $1.columnName() }
      | '-' b_expr              { $$.val = &tree.FuncExpr{Name: "-", Args: tree.ExprStatements{$2.exprStatement()}} }
      | b_expr '+' b_expr       { $$.val = &tree.FuncExpr{Name: "+", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | b_expr '-' b_expr       { $$.val = &tree.FuncExpr{Name: "-", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | b_expr '*' b_expr       { $$.val = &tree.FuncExpr{Name: "*", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | b_expr '/' b_expr       { $$.val = &tree.FuncExpr{Name: "/", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | b_expr '%' b_expr       { $$.val = &tree.FuncExpr{Name: "%", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | func_expr               { $$.val = $1.funcStatement() }

c_expr: b_expr '<' b_expr                           { $$.val = &tree.FuncExpr{Name: "<", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | b_expr '>' b_expr                           { $$.val = &tree.FuncExpr{Name: ">", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | b_expr '=' b_expr                           { $$.val = &tree.FuncExpr{Name: "=", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | b_expr LESS_EQUALS b_expr                   { $$.val = &tree.FuncExpr{Name: "<=", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | b_expr GREATER_EQUALS b_expr                { $$.val = &tree.FuncExpr{Name: ">=", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }
      | b_expr NOT_EQUALS b_expr                    { $$.val = &tree.FuncExpr{Name: "<>", Args: tree.ExprStatements{$1.exprStatement(), $3.exprStatement()}} }

d_expr: ICONST          { $$.val = $1.valueStatement() }
      | FCONST          { $$.val = $1.valueStatement() }
      | SCONST          { $$.val = $1.valueStatement() }
      | TRUE            { $$.val = &tree.Value{Value: constant.MakeBool(true)} }
      | FALSE           { $$.val = &tree.Value{Value: constant.MakeBool(false)} }
      | '(' a_expr ')'  { $$.val = &tree.ParenExpr{$2.exprStatement()} }

signed_iconst: ICONST       { $$.val = $1.valueStatement() }
             | '+' ICONST   { $$.val = $2.valueStatement() }
             | '-' ICONST   { $$.val = $2.setNegative() }

func_expr: func_application
           {
                $$.val = $1.funcStatement()
           }
         | func_expr_common_subexpr
           {
                $$.val = $1.funcStatement()
           }

func_application: func_name '(' ')'
                  {
                    $$.val = &tree.FuncExpr{Name: $1}
                  }
                | func_name '(' expr_list ')'
                  {
                    $$.val = &tree.FuncExpr{Name: $1, Args: $3.exprStatements() }
                  }

func_expr_common_subexpr: CAST '(' a_expr AS cast_target ')'
                          {
                            $$.val = &tree.FuncExpr{Name: "cast", Args: tree.ExprStatements{$3.exprStatement(), $5.exprStatement()} }
                          }

cast_target: typename { $$.val = $1.exprStatement() }

typename: INT       { $$.val = &tree.Value{Value: constant.MakeInt64(0)} }
        | LONG      { $$.val = &tree.Value{Value: constant.MakeInt64(0)} }
        | BOOL      { $$.val = &tree.Value{Value: constant.MakeBool(true)} }
        | FLOAT     { $$.val = &tree.Value{Value: constant.MakeFloat64(0)} }
        | DOUBLE    { $$.val = &tree.Value{Value: constant.MakeFloat64(0)} }
        | STRING    { $$.val = &tree.Value{Value: constant.MakeString("")} }

column_list: column_name
             {
                $$.val = tree.ColumnNameList{$1.columnName()}
             }
           | column_list ',' column_name
             {
                $$.val = append($1.columnNameList(), $3.columnName())
             }

column_name: name
             {
                $$.val = tree.ColumnName{Path: tree.Name($1) }
             }

name: IDENT

func_name: name

%%
