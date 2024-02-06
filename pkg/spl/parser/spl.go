// Code generated by goyacc -o spl.go -p spl -v spl.output spl.y. DO NOT EDIT.

//line spl.y:2

package parser

import __yyfmt__ "fmt"

//line spl.y:3

import (
	"github.com/nnsgmsone/nexus/pkg/spl/tree"
	"go/constant"
)

//line spl.y:13

type splSymUnion struct {
	val any
	ss  *tree.Select
}

func (u *splSymUnion) SelectStatement() *tree.Select {
	if u.ss == nil {
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

func (u *splSymUnion) evalExprStatement() tree.EvalExpr {
	return u.val.(tree.EvalExpr)
}

func (u *splSymUnion) evalExprListStatement() tree.EvalExprList {
	return u.val.(tree.EvalExprList)
}

func (u *splSymUnion) exprStatement() tree.ExprStatement {
	return u.val.(tree.ExprStatement)
}

func (u *splSymUnion) exprStatements() tree.ExprStatements {
	return u.val.(tree.ExprStatements)
}

func (u *splSymUnion) valueStatement() *tree.Value {
	if u.val == nil {
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

func (u *splSymUnion) orderStatement() *tree.Order {
	return u.val.(*tree.Order)
}

func (u *splSymUnion) direction() tree.Direction {
	return u.val.(tree.Direction)
}

func (u *splSymUnion) tableName() *tree.TableName {
	return u.val.(*tree.TableName)
}

func (u *splSymUnion) columnName() tree.ColumnName {
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

func (u *splSymUnion) setNegative() *tree.Value {
	v, ok := u.val.(*tree.Value)
	if !ok {
		return nil
	}
	iv, _ := constant.Int64Val(v.Value)
	v.Value = constant.MakeInt64(-1 * iv)
	return v
}

//line spl.y:187
type splSymType struct {
	yys   int
	id    int32
	pos   int32
	byt   byte
	str   string
	union splSymUnion
}

const IDENT = 57346
const ICONST = 57347
const FCONST = 57348
const SCONST = 57349
const LESS_EQUALS = 57350
const GREATER_EQUALS = 57351
const NOT_EQUALS = 57352
const AND = 57353
const AS = 57354
const ASC = 57355
const BOOL = 57356
const BY = 57357
const CAST = 57358
const DESC = 57359
const DEDUP = 57360
const DOUBLE = 57361
const EVAL = 57362
const FALSE = 57363
const FLOAT = 57364
const INT = 57365
const IMPORT = 57366
const LIMIT = 57367
const LONG = 57368
const LUA = 57369
const LUA_FILE = 57370
const NOT = 57371
const OR = 57372
const ORDER = 57373
const EXTRACT = 57374
const STATS = 57375
const STRING = 57376
const TRUE = 57377
const TYPE = 57378
const WHERE = 57379

var splToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"IDENT",
	"ICONST",
	"FCONST",
	"SCONST",
	"LESS_EQUALS",
	"GREATER_EQUALS",
	"NOT_EQUALS",
	"AND",
	"AS",
	"ASC",
	"BOOL",
	"BY",
	"CAST",
	"DESC",
	"DEDUP",
	"DOUBLE",
	"EVAL",
	"FALSE",
	"FLOAT",
	"INT",
	"IMPORT",
	"LIMIT",
	"LONG",
	"LUA",
	"LUA_FILE",
	"NOT",
	"OR",
	"ORDER",
	"EXTRACT",
	"STATS",
	"STRING",
	"TRUE",
	"TYPE",
	"WHERE",
	"'+'",
	"'-'",
	"'*'",
	"'/'",
	"'%'",
	"'<'",
	"'>'",
	"'='",
	"'('",
	"')'",
	"'|'",
	"':'",
	"','",
}

var splStatenames = [...]string{}

const splEofCode = 1
const splErrCode = 2
const splInitialStackSize = 16

//line spl.y:524

//line yacctab:1
var splExca = [...]int8{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 53,
	46, 88,
	-2, 86,
}

const splPrivate = 57344

const splLast = 232

var splAct = [...]uint8{
	53, 72, 111, 42, 44, 109, 36, 56, 135, 132,
	26, 133, 133, 75, 96, 27, 102, 67, 38, 97,
	60, 39, 21, 27, 59, 37, 58, 89, 90, 91,
	71, 79, 2, 149, 60, 95, 94, 77, 73, 81,
	82, 83, 84, 85, 80, 68, 79, 66, 76, 92,
	78, 65, 83, 84, 85, 93, 74, 81, 82, 83,
	84, 85, 86, 87, 88, 78, 137, 125, 27, 23,
	24, 101, 134, 25, 79, 60, 60, 38, 100, 73,
	59, 112, 113, 108, 37, 114, 115, 116, 117, 118,
	119, 120, 121, 122, 123, 124, 64, 60, 128, 4,
	127, 129, 107, 60, 131, 106, 70, 73, 28, 47,
	48, 49, 104, 30, 69, 30, 105, 29, 79, 136,
	57, 63, 130, 33, 99, 51, 98, 62, 28, 47,
	48, 49, 28, 41, 34, 139, 138, 78, 148, 50,
	57, 46, 55, 45, 54, 51, 31, 32, 31, 32,
	52, 126, 140, 41, 141, 28, 47, 48, 49, 50,
	35, 43, 40, 45, 61, 103, 12, 57, 22, 5,
	52, 110, 51, 11, 10, 28, 47, 48, 49, 9,
	41, 8, 7, 6, 3, 1, 50, 57, 0, 0,
	45, 0, 51, 0, 19, 0, 14, 52, 0, 0,
	20, 15, 0, 0, 0, 0, 50, 16, 13, 17,
	45, 144, 0, 18, 0, 0, 146, 52, 0, 145,
	142, 0, 0, 143, 0, 0, 0, 0, 0, 0,
	0, 147,
}

var splPact = [...]int16{
	-16, -1000, 176, -26, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 42, 128, 110, 108, 128, 151, 128,
	120, 176, 128, 6, 2, -33, -1000, 0, -1000, -1000,
	-1000, 109, 101, 128, 41, -2, -1000, -9, -1000, 35,
	-1000, 151, 19, -1000, -1000, 171, -1000, -1000, -1000, -1000,
	-1000, -1000, 151, -1000, -1000, -1000, -10, -11, -36, -1000,
	-1000, -31, -1000, -1000, -33, 119, 117, 128, 151, -1000,
	-1000, -34, -1000, 99, 128, 128, 128, 124, 151, 151,
	-1000, 171, 171, 171, 171, 171, 171, 171, 171, 171,
	171, 171, 12, 20, 104, 151, 128, 115, -1000, -1000,
	-1000, 35, 128, -1000, -1000, -1000, -34, -36, -1000, -38,
	60, 35, 63, -1000, 12, 12, -1000, -1000, -1000, 1,
	1, 1, 1, 1, 1, -1000, -1000, -39, 107, -1000,
	-1000, -1000, 54, 151, 128, -1000, 197, 128, 35, -1000,
	-14, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
}

var splPgo = [...]uint8{
	0, 185, 184, 99, 183, 182, 181, 179, 174, 173,
	169, 168, 166, 165, 73, 10, 0, 7, 4, 26,
	164, 30, 5, 2, 3, 162, 161, 1, 160, 6,
	154, 152, 117, 144, 142, 141,
}

var splR1 = [...]int8{
	0, 1, 2, 2, 3, 3, 3, 3, 3, 3,
	3, 3, 10, 11, 11, 4, 8, 5, 9, 7,
	7, 12, 20, 20, 14, 14, 15, 28, 28, 29,
	29, 29, 29, 6, 6, 21, 21, 27, 13, 13,
	13, 22, 22, 23, 23, 23, 23, 23, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 25, 25, 25,
	25, 25, 25, 26, 26, 26, 26, 26, 26, 32,
	32, 32, 35, 35, 33, 33, 34, 31, 30, 30,
	30, 30, 30, 30, 19, 19, 18, 16, 17,
}

var splR2 = [...]int8{
	0, 2, 1, 3, 1, 1, 1, 1, 1, 1,
	1, 1, 3, 3, 3, 2, 2, 2, 2, 2,
	4, 2, 1, 3, 1, 3, 3, 1, 3, 6,
	5, 3, 4, 3, 4, 1, 3, 2, 1, 1,
	0, 1, 3, 1, 2, 3, 3, 1, 1, 1,
	2, 3, 3, 3, 3, 3, 1, 3, 3, 3,
	3, 3, 3, 1, 1, 1, 1, 1, 3, 1,
	2, 2, 1, 1, 3, 4, 6, 1, 1, 1,
	1, 1, 1, 1, 1, 3, 1, 1, 1,
}

var splChk = [...]int16{
	-1000, -1, 48, -2, -3, -10, -4, -5, -6, -7,
	-8, -9, -12, 32, 20, 25, 31, 33, 37, 18,
	24, 48, -11, 27, 28, -14, -15, -16, 4, -32,
	5, 38, 39, 15, -32, -28, -29, -17, -16, -23,
	-25, 29, -24, -26, -18, 39, -35, 5, 6, 7,
	35, 21, 46, -16, -33, -34, -17, 16, -19, -18,
	-16, -20, 7, -3, -14, 45, 45, 50, 45, 5,
	5, -21, -27, -18, 15, 15, 50, 46, 30, 11,
	-23, 38, 39, 40, 41, 42, 43, 44, 45, 8,
	9, 10, -24, -23, 46, 46, 50, 50, 7, 7,
	-15, -23, 50, -13, 13, 17, -21, -19, -29, -22,
	47, -23, -23, -23, -24, -24, -24, -24, -24, -24,
	-24, -24, -24, -24, -24, 47, 47, -22, -23, -18,
	7, -27, 47, 50, 12, 47, 12, 12, -23, -16,
	-31, -30, 23, 26, 14, 22, 19, 34, -16, 47,
}

var splDef = [...]int8{
	0, -2, 0, 1, 2, 4, 5, 6, 7, 8,
	9, 10, 11, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 15, 24, 0, 87, 17,
	69, 0, 0, 0, 0, 19, 27, 0, 88, 16,
	43, 0, 47, 48, 49, 0, 56, 63, 64, 65,
	66, 67, 0, -2, 72, 73, 0, 0, 18, 84,
	86, 21, 22, 3, 12, 0, 0, 0, 0, 70,
	71, 33, 35, 40, 0, 0, 0, 0, 0, 0,
	44, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 50, 0, 0, 0, 0, 0, 13, 14,
	25, 26, 0, 37, 38, 39, 34, 20, 28, 0,
	31, 41, 45, 46, 51, 52, 53, 54, 55, 57,
	58, 59, 60, 61, 62, 68, 74, 0, 0, 85,
	23, 36, 32, 0, 0, 75, 0, 0, 42, 30,
	0, 77, 78, 79, 80, 81, 82, 83, 29, 76,
}

var splTok1 = [...]int8{
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 42, 3, 3,
	46, 47, 40, 38, 50, 39, 3, 41, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 49, 3,
	43, 45, 44, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 48,
}

var splTok2 = [...]int8{
	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37,
}

var splTok3 = [...]int8{
	0,
}

var splErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

//line yaccpar:1

/*	parser for yacc output	*/

var (
	splDebug        = 0
	splErrorVerbose = false
)

type splLexer interface {
	Lex(lval *splSymType) int
	Error(s string)
}

type splParser interface {
	Parse(splLexer) int
	Lookahead() int
}

type splParserImpl struct {
	lval  splSymType
	stack [splInitialStackSize]splSymType
	char  int
}

func (p *splParserImpl) Lookahead() int {
	return p.char
}

func splNewParser() splParser {
	return &splParserImpl{}
}

const splFlag = -1000

func splTokname(c int) string {
	if c >= 1 && c-1 < len(splToknames) {
		if splToknames[c-1] != "" {
			return splToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func splStatname(s int) string {
	if s >= 0 && s < len(splStatenames) {
		if splStatenames[s] != "" {
			return splStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func splErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !splErrorVerbose {
		return "syntax error"
	}

	for _, e := range splErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + splTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := int(splPact[state])
	for tok := TOKSTART; tok-1 < len(splToknames); tok++ {
		if n := base + tok; n >= 0 && n < splLast && int(splChk[int(splAct[n])]) == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if splDef[state] == -2 {
		i := 0
		for splExca[i] != -1 || int(splExca[i+1]) != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; splExca[i] >= 0; i += 2 {
			tok := int(splExca[i])
			if tok < TOKSTART || splExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if splExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += splTokname(tok)
	}
	return res
}

func spllex1(lex splLexer, lval *splSymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = int(splTok1[0])
		goto out
	}
	if char < len(splTok1) {
		token = int(splTok1[char])
		goto out
	}
	if char >= splPrivate {
		if char < splPrivate+len(splTok2) {
			token = int(splTok2[char-splPrivate])
			goto out
		}
	}
	for i := 0; i < len(splTok3); i += 2 {
		token = int(splTok3[i+0])
		if token == char {
			token = int(splTok3[i+1])
			goto out
		}
	}

out:
	if token == 0 {
		token = int(splTok2[1]) /* unknown char */
	}
	if splDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", splTokname(token), uint(char))
	}
	return char, token
}

func splParse(spllex splLexer) int {
	return splNewParser().Parse(spllex)
}

func (splrcvr *splParserImpl) Parse(spllex splLexer) int {
	var spln int
	var splVAL splSymType
	var splDollar []splSymType
	_ = splDollar // silence set and not used
	splS := splrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	splstate := 0
	splrcvr.char = -1
	spltoken := -1 // splrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		splstate = -1
		splrcvr.char = -1
		spltoken = -1
	}()
	splp := -1
	goto splstack

ret0:
	return 0

ret1:
	return 1

splstack:
	/* put a state and value onto the stack */
	if splDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", splTokname(spltoken), splStatname(splstate))
	}

	splp++
	if splp >= len(splS) {
		nyys := make([]splSymType, len(splS)*2)
		copy(nyys, splS)
		splS = nyys
	}
	splS[splp] = splVAL
	splS[splp].yys = splstate

splnewstate:
	spln = int(splPact[splstate])
	if spln <= splFlag {
		goto spldefault /* simple state */
	}
	if splrcvr.char < 0 {
		splrcvr.char, spltoken = spllex1(spllex, &splrcvr.lval)
	}
	spln += spltoken
	if spln < 0 || spln >= splLast {
		goto spldefault
	}
	spln = int(splAct[spln])
	if int(splChk[spln]) == spltoken { /* valid shift */
		splrcvr.char = -1
		spltoken = -1
		splVAL = splrcvr.lval
		splstate = spln
		if Errflag > 0 {
			Errflag--
		}
		goto splstack
	}

spldefault:
	/* default state action */
	spln = int(splDef[splstate])
	if spln == -2 {
		if splrcvr.char < 0 {
			splrcvr.char, spltoken = spllex1(spllex, &splrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if splExca[xi+0] == -1 && int(splExca[xi+1]) == splstate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			spln = int(splExca[xi+0])
			if spln < 0 || spln == spltoken {
				break
			}
		}
		spln = int(splExca[xi+1])
		if spln < 0 {
			goto ret0
		}
	}
	if spln == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			spllex.Error(splErrorMessage(splstate, spltoken))
			Nerrs++
			if splDebug >= 1 {
				__yyfmt__.Printf("%s", splStatname(splstate))
				__yyfmt__.Printf(" saw %s\n", splTokname(spltoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for splp >= 0 {
				spln = int(splPact[splS[splp].yys]) + splErrCode
				if spln >= 0 && spln < splLast {
					splstate = int(splAct[spln]) /* simulate a shift of "error" */
					if int(splChk[splstate]) == splErrCode {
						goto splstack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if splDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", splS[splp].yys)
				}
				splp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if splDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", splTokname(spltoken))
			}
			if spltoken == splEofCode {
				goto ret1
			}
			splrcvr.char = -1
			spltoken = -1
			goto splnewstate /* try again in the same state */
		}
	}

	/* reduction by production spln */
	if splDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", spln, splStatname(splstate))
	}

	splnt := spln
	splpt := splp
	_ = splpt // guard against "declared and not used"

	splp -= int(splR2[spln])
	// splp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if splp+1 >= len(splS) {
		nyys := make([]splSymType, len(splS)*2)
		copy(nyys, splS)
		splS = nyys
	}
	splVAL = splS[splp+1]

	/* consult goto table to find next state */
	spln = int(splR1[spln])
	splg := int(splPgo[spln])
	splj := splg + splS[splp].yys + 1

	if splj >= splLast {
		splstate = int(splAct[splg])
	} else {
		splstate = int(splAct[splj])
		if int(splChk[splstate]) != -spln {
			splstate = int(splAct[splg])
		}
	}
	// dummy call; replaced with literal code
	switch splnt {

	case 1:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:250
		{
			spllex.(*lexer).SetStmt(splDollar[2].union.SelectStatement())
		}
	case 2:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:255
		{
			splVAL.union.val = splDollar[1].union.SelectStatement()
		}
	case 3:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:259
		{
			as, bs := splDollar[1].union.SelectStatement(), splDollar[3].union.SelectStatement()
			as.Cs = append(as.Cs, bs.Cs...)
			splVAL.union.val = as
		}
	case 4:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:266
		{
			ss := splVAL.union.SelectStatement()
			ss.Cs = append(ss.Cs, splDollar[1].union.ExtractStatement())
		}
	case 5:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:271
		{
			ss := splVAL.union.SelectStatement()
			ss.Cs = append(ss.Cs, splDollar[1].union.EvalStatement())
		}
	case 6:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:276
		{
			ss := splVAL.union.SelectStatement()
			ss.Cs = append(ss.Cs, splDollar[1].union.LimitStatement())
		}
	case 7:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:281
		{
			ss := splVAL.union.SelectStatement()
			ss.Cs = append(ss.Cs, splDollar[1].union.OrderStatement())
		}
	case 8:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:286
		{
			ss := splVAL.union.SelectStatement()
			ss.Cs = append(ss.Cs, splDollar[1].union.StatsStatement())
		}
	case 9:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:291
		{
			ss := splVAL.union.SelectStatement()
			ss.Cs = append(ss.Cs, splDollar[1].union.WhereStatement())
		}
	case 10:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:296
		{
			ss := splVAL.union.SelectStatement()
			ss.Cs = append(ss.Cs, splDollar[1].union.StatsStatement())
		}
	case 11:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:301
		{
			ss := splVAL.union.SelectStatement()
			ss.Cs = append(ss.Cs, splDollar[1].union.ImportStatement())
		}
	case 12:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:307
		{
			splVAL.union.val = &tree.Extract{Script: splDollar[2].union.extractScriptOpt(), Es: splDollar[3].union.evalExprListStatement()}
		}
	case 13:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:312
		{
			splVAL.union.val = tree.ExtractScript{Lua: splDollar[3].union.valueStatement()}
		}
	case 14:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:316
		{
			splVAL.union.val = tree.ExtractScript{LuaFile: splDollar[3].union.valueStatement()}
		}
	case 15:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:322
		{
			splVAL.union.val = &tree.Eval{splDollar[2].union.evalExprListStatement()}
		}
	case 16:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:327
		{
			splVAL.union.val = &tree.Where{splDollar[2].union.exprStatement()}
		}
	case 17:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:332
		{
			splVAL.union.val = &tree.Limit{Count: splDollar[2].union.valueStatement()}
		}
	case 18:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:337
		{
			splVAL.union.val = &tree.Stats{By: splDollar[2].union.columnNameList()}
		}
	case 19:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:343
		{
			splVAL.union.val = &tree.Stats{Ss: splDollar[2].union.statListStatement()}
		}
	case 20:
		splDollar = splS[splpt-4 : splpt+1]
//line spl.y:347
		{
			splVAL.union.val = &tree.Stats{Ss: splDollar[2].union.statListStatement(), By: splDollar[4].union.columnNameList()}
		}
	case 21:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:352
		{
			splVAL.union.val = &tree.Import{Paths: splDollar[2].union.valueStatements()}
		}
	case 22:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:357
		{
			splVAL.union.val = []*tree.Value{splDollar[1].union.valueStatement()}
		}
	case 23:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:361
		{
			splVAL.union.val = append(splDollar[1].union.valueStatements(), splDollar[3].union.valueStatement())
		}
	case 24:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:366
		{
			splVAL.union.val = tree.EvalExprList{splDollar[1].union.evalExprStatement()}
		}
	case 25:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:370
		{
			splVAL.union.val = append(splDollar[1].union.evalExprListStatement(), splDollar[3].union.evalExprStatement())
		}
	case 26:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:375
		{
			splVAL.union.val = tree.EvalExpr{As: tree.Name(splDollar[1].str), E: splDollar[3].union.exprStatement()}
		}
	case 27:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:380
		{
			splVAL.union.val = tree.StatList{splDollar[1].union.statStatement()}
		}
	case 28:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:384
		{
			splVAL.union.val = append(splDollar[1].union.statListStatement(), splDollar[3].union.statStatement())
		}
	case 29:
		splDollar = splS[splpt-6 : splpt+1]
//line spl.y:389
		{
			splVAL.union.val = tree.Stat{F: tree.Name(splDollar[1].str), Es: splDollar[3].union.exprStatements(), As: tree.Name(splDollar[6].str)}
		}
	case 30:
		splDollar = splS[splpt-5 : splpt+1]
//line spl.y:393
		{
			splVAL.union.val = tree.Stat{F: tree.Name(splDollar[1].str), As: tree.Name(splDollar[5].str)}
		}
	case 31:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:397
		{
			splVAL.union.val = tree.Stat{F: tree.Name(splDollar[1].str)}
		}
	case 32:
		splDollar = splS[splpt-4 : splpt+1]
//line spl.y:401
		{
			splVAL.union.val = tree.Stat{F: tree.Name(splDollar[1].str), Es: splDollar[3].union.exprStatements()}
		}
	case 33:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:406
		{
			splVAL.union.val = &tree.OrderBy{Orders: splDollar[3].union.orderListStatement(), Limit: nil}
		}
	case 34:
		splDollar = splS[splpt-4 : splpt+1]
//line spl.y:410
		{
			splVAL.union.val = &tree.OrderBy{Limit: splDollar[2].union.valueStatement(), Orders: splDollar[4].union.orderListStatement()}
		}
	case 35:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:415
		{
			splVAL.union.val = tree.OrderList{splDollar[1].union.orderStatement()}
		}
	case 36:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:419
		{
			splVAL.union.val = append(splDollar[1].union.orderListStatement(), splDollar[3].union.orderStatement())
		}
	case 37:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:424
		{
			splVAL.union.val = &tree.Order{
				E:    splDollar[1].union.exprStatement(),
				Type: splDollar[2].union.direction(),
			}
		}
	case 38:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:431
		{
			splVAL.union.val = tree.Ascending
		}
	case 39:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:432
		{
			splVAL.union.val = tree.Descending
		}
	case 40:
		splDollar = splS[splpt-0 : splpt+1]
//line spl.y:433
		{
			splVAL.union.val = tree.DefaultDirection
		}
	case 41:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:437
		{
			splVAL.union.val = tree.ExprStatements{splDollar[1].union.exprStatement()}
		}
	case 42:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:438
		{
			splVAL.union.val = append(splDollar[1].union.exprStatements(), splDollar[3].union.exprStatement())
		}
	case 43:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:440
		{
			splVAL.union.val = splDollar[1].union.exprStatement()
		}
	case 44:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:441
		{
			splVAL.union.val = &tree.FuncExpr{Name: "not", Args: tree.ExprStatements{splDollar[2].union.exprStatement()}}
		}
	case 45:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:442
		{
			splVAL.union.val = &tree.FuncExpr{Name: "or", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 46:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:443
		{
			splVAL.union.val = &tree.FuncExpr{Name: "and", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 47:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:444
		{
			splVAL.union.val = splDollar[1].union.exprStatement()
		}
	case 48:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:446
		{
			splVAL.union.val = splDollar[1].union.exprStatement()
		}
	case 49:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:447
		{
			splVAL.union.val = splDollar[1].union.columnName()
		}
	case 50:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:448
		{
			splVAL.union.val = &tree.FuncExpr{Name: "-", Args: tree.ExprStatements{splDollar[2].union.exprStatement()}}
		}
	case 51:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:449
		{
			splVAL.union.val = &tree.FuncExpr{Name: "+", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 52:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:450
		{
			splVAL.union.val = &tree.FuncExpr{Name: "-", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 53:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:451
		{
			splVAL.union.val = &tree.FuncExpr{Name: "*", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 54:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:452
		{
			splVAL.union.val = &tree.FuncExpr{Name: "/", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 55:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:453
		{
			splVAL.union.val = &tree.FuncExpr{Name: "%", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 56:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:454
		{
			splVAL.union.val = splDollar[1].union.funcStatement()
		}
	case 57:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:456
		{
			splVAL.union.val = &tree.FuncExpr{Name: "<", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 58:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:457
		{
			splVAL.union.val = &tree.FuncExpr{Name: ">", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 59:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:458
		{
			splVAL.union.val = &tree.FuncExpr{Name: "=", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 60:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:459
		{
			splVAL.union.val = &tree.FuncExpr{Name: "<=", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 61:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:460
		{
			splVAL.union.val = &tree.FuncExpr{Name: ">=", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 62:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:461
		{
			splVAL.union.val = &tree.FuncExpr{Name: "<>", Args: tree.ExprStatements{splDollar[1].union.exprStatement(), splDollar[3].union.exprStatement()}}
		}
	case 63:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:463
		{
			splVAL.union.val = splDollar[1].union.valueStatement()
		}
	case 64:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:464
		{
			splVAL.union.val = splDollar[1].union.valueStatement()
		}
	case 65:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:465
		{
			splVAL.union.val = splDollar[1].union.valueStatement()
		}
	case 66:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:466
		{
			splVAL.union.val = &tree.Value{Value: constant.MakeBool(true)}
		}
	case 67:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:467
		{
			splVAL.union.val = &tree.Value{Value: constant.MakeBool(false)}
		}
	case 68:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:468
		{
			splVAL.union.val = &tree.ParenExpr{splDollar[2].union.exprStatement()}
		}
	case 69:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:470
		{
			splVAL.union.val = splDollar[1].union.valueStatement()
		}
	case 70:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:471
		{
			splVAL.union.val = splDollar[2].union.valueStatement()
		}
	case 71:
		splDollar = splS[splpt-2 : splpt+1]
//line spl.y:472
		{
			splVAL.union.val = splDollar[2].union.setNegative()
		}
	case 72:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:475
		{
			splVAL.union.val = splDollar[1].union.funcStatement()
		}
	case 73:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:479
		{
			splVAL.union.val = splDollar[1].union.funcStatement()
		}
	case 74:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:484
		{
			splVAL.union.val = &tree.FuncExpr{Name: splDollar[1].str}
		}
	case 75:
		splDollar = splS[splpt-4 : splpt+1]
//line spl.y:488
		{
			splVAL.union.val = &tree.FuncExpr{Name: splDollar[1].str, Args: splDollar[3].union.exprStatements()}
		}
	case 76:
		splDollar = splS[splpt-6 : splpt+1]
//line spl.y:493
		{
			splVAL.union.val = &tree.FuncExpr{Name: "cast", Args: tree.ExprStatements{splDollar[3].union.exprStatement(), splDollar[5].union.exprStatement()}}
		}
	case 77:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:497
		{
			splVAL.union.val = splDollar[1].union.exprStatement()
		}
	case 78:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:499
		{
			splVAL.union.val = &tree.Value{Value: constant.MakeInt64(0)}
		}
	case 79:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:500
		{
			splVAL.union.val = &tree.Value{Value: constant.MakeInt64(0)}
		}
	case 80:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:501
		{
			splVAL.union.val = &tree.Value{Value: constant.MakeBool(true)}
		}
	case 81:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:502
		{
			splVAL.union.val = &tree.Value{Value: constant.MakeFloat64(0)}
		}
	case 82:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:503
		{
			splVAL.union.val = &tree.Value{Value: constant.MakeFloat64(0)}
		}
	case 83:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:504
		{
			splVAL.union.val = &tree.Value{Value: constant.MakeString("")}
		}
	case 84:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:507
		{
			splVAL.union.val = tree.ColumnNameList{splDollar[1].union.columnName()}
		}
	case 85:
		splDollar = splS[splpt-3 : splpt+1]
//line spl.y:511
		{
			splVAL.union.val = append(splDollar[1].union.columnNameList(), splDollar[3].union.columnName())
		}
	case 86:
		splDollar = splS[splpt-1 : splpt+1]
//line spl.y:516
		{
			splVAL.union.val = tree.ColumnName{Path: tree.Name(splDollar[1].str)}
		}
	}
	goto splstack /* stack new state and value */
}