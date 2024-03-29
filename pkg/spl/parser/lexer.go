package parser

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/nnsgmsone/nexus/pkg/spl/tree"
)

const ERROR = 57345

type lexer struct {
	in string

	// tokens contains tokens generated by the scanner.
	tokens []splSymType

	// lastPos is the position into the tokens slice of the last
	// token returned by Lex().
	lastPos int

	stmt *tree.Select

	lastError error
}

func (l *lexer) init(spl string, tokens []splSymType) {
	l.in = spl
	l.tokens = tokens
	l.lastPos = -1
	l.stmt = nil
	l.lastError = nil
}

// cleanup is used to avoid holding on to memory unnecessarily (for the cases
// where we reuse a scanner).
func (l *lexer) cleanup() {
	l.tokens = nil
	l.stmt = nil
	l.lastError = nil
}

// Lex lexes a token from input.
func (l *lexer) Lex(lval *splSymType) int {
	l.lastPos++
	// The core lexing takes place in the scanner. Here we do a small bit of post
	// processing of the lexical tokens so that the grammar only requires
	// one-token lookahead despite SPL requiring multi-token lookahead in some
	// cases. These special cases are handled below and the returned tokens are
	// adjusted to reflect the lookahead (LA) that occurred.
	if l.lastPos >= len(l.tokens) {
		lval.id = 0
		lval.pos = int32(len(l.in))
		lval.str = "EOF"
		return 0
	}
	*lval = l.tokens[l.lastPos]

	return int(lval.id)
}

func (l *lexer) lastToken() splSymType {
	if l.lastPos < 0 {
		return splSymType{}
	}

	if l.lastPos >= len(l.tokens) {
		return splSymType{
			id:  0,
			pos: int32(len(l.in)),
			str: "EOF",
		}
	}
	return l.tokens[l.lastPos]
}

// SetStmt is called from the parser when the statement is constructed.
func (l *lexer) SetStmt(stmt *tree.Select) {
	l.stmt = stmt
}

// Unimplemented wraps Error, setting lastUnimplementedError.
func (l *lexer) Unimplemented(feature string) {
	l.lastError = fmt.Errorf("unimplemented: %s this syntax", feature)
	l.populateErrorDetails()
}

// while running the action. That error becomes the actual "cause" of the
// syntax error.
func (l *lexer) setErr(err error) {
	l.lastError = err
	l.populateErrorDetails()
}

func (l *lexer) Error(e string) {
	e = strings.TrimPrefix(e, "syntax error: ") // we'll add it again below.
	l.lastError = errors.New(e)
	l.populateErrorDetails()
}

func (l *lexer) populateErrorDetails() {
	lastTok := l.lastToken()

	if lastTok.id == ERROR {
		// This is a tokenizer (lexical) error: the scanner
		// will have stored the error message in the string field.
		l.lastError = fmt.Errorf("lexical error: %s", lastTok.str)
	}
	// Find the end of the line containing the last token.
	i := strings.IndexByte(l.in[lastTok.pos:], '\n')
	if i == -1 {
		i = len(l.in)
	} else {
		i += int(lastTok.pos)
	}
	// Find the beginning of the line containing the last token. Note that
	// LastIndexByte returns -1 if '\n' could not be found.
	j := strings.LastIndexByte(l.in[:lastTok.pos], '\n') + 1
	var buf bytes.Buffer
	// Output everything up to and including the line containing the last token.
	fmt.Fprintf(&buf, "source SPL:\n%s\n", l.in[:i])
	// Output a caret indicating where the last token starts.
	fmt.Fprintf(&buf, "%s^", strings.Repeat(" ", int(lastTok.pos)-j))
	l.lastError = fmt.Errorf("%v: %v", l.lastError, buf.String())
}
