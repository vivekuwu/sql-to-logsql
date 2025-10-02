package lexer

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/VictoriaMetrics/sql-to-logsql/lib/sql/token"
)

// Lexer converts raw SQL text into a stream of tokens.
type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           rune
	line         int
	column       int
}

// New creates a new Lexer instance.
func New(input string) *Lexer {
	l := &Lexer{input: input, line: 1}
	l.readRune()
	return l
}

// NextToken advances and returns the next token from the input.
func (l *Lexer) NextToken() token.Token {
	l.skipWhitespace()
	l.skipComments()
	l.skipWhitespace()

	startPos := token.Position{Line: l.line, Column: l.column}
	tok := token.Token{Type: token.ILLEGAL, Literal: string(l.ch), Pos: startPos}

	switch l.ch {
	case 0:
		tok.Type = token.EOF
		tok.Literal = ""
	case ',':
		tok = l.makeSimple(token.COMMA, startPos)
	case ';':
		tok = l.makeSimple(token.SEMICOLON, startPos)
	case '(':
		tok = l.makeSimple(token.LPAREN, startPos)
	case ')':
		tok = l.makeSimple(token.RPAREN, startPos)
	case '[':
		tok = l.makeSimple(token.LBRACKET, startPos)
	case ']':
		tok = l.makeSimple(token.RBRACKET, startPos)
	case '.':
		tok = l.makeSimple(token.DOT, startPos)
	case '*':
		tok = l.makeSimple(token.STAR, startPos)
	case '+':
		tok = l.makeSimple(token.PLUS, startPos)
	case '-':
		if l.peekRune() == '-' {
			l.readRune()
			l.consumeLine()
			return l.NextToken()
		}
		tok = l.makeSimple(token.MINUS, startPos)
	case '/':
		if l.peekRune() == '*' {
			l.readRune()
			l.readRune()
			l.consumeBlockComment()
			return l.NextToken()
		}
		tok = l.makeSimple(token.SLASH, startPos)
	case '%':
		tok = l.makeSimple(token.PERCENT, startPos)
	case '^':
		tok = l.makeSimple(token.CARET, startPos)
	case '=':
		tok = l.makeSimple(token.EQ, startPos)
	case '!':
		if l.peekRune() == '=' {
			ch := l.ch
			l.readRune()
			literal := string([]rune{ch, l.ch})
			tok = token.Token{Type: token.NEQ, Literal: literal, Pos: startPos}
		}
	case '<':
		if l.peekRune() == '=' {
			tok = token.Token{Type: token.LTE, Literal: "<=", Pos: startPos}
			l.readRune()
		} else if l.peekRune() == '>' {
			tok = token.Token{Type: token.NEQ, Literal: "<>", Pos: startPos}
			l.readRune()
		} else {
			tok = l.makeSimple(token.LT, startPos)
		}
	case '>':
		if l.peekRune() == '=' {
			tok = token.Token{Type: token.GTE, Literal: ">=", Pos: startPos}
			l.readRune()
		} else {
			tok = l.makeSimple(token.GT, startPos)
		}
	case '?':
		tok = l.makeSimple(token.PLACEHOLDER, startPos)
	case '\'':
		literal := l.readString('\'')
		tok = token.Token{Type: token.STRING, Literal: literal, Pos: startPos}
		return tok
	case '"':
		literal := l.readQuotedIdentifier()
		tok = token.Token{Type: token.IDENT, Literal: literal, Pos: startPos}
		return tok
	default:
		if isIdentStart(l.ch) {
			ident := l.readIdentifier()
			upper := strings.ToUpper(ident)
			tokType := token.Lookup(upper)
			if tokType == token.IDENT {
				return token.Token{Type: token.IDENT, Literal: ident, Pos: startPos}
			}
			return token.Token{Type: tokType, Literal: upper, Pos: startPos}
		}
		if unicode.IsDigit(l.ch) {
			number := l.readNumber()
			tok = token.Token{Type: token.NUMBER, Literal: number, Pos: startPos}
			return tok
		}
	}

	l.readRune()
	return tok
}

func (l *Lexer) makeSimple(t token.Type, pos token.Position) token.Token {
	tok := token.Token{Type: t, Literal: string(l.ch), Pos: pos}
	return tok
}

func (l *Lexer) readRune() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
		l.position = l.readPosition
		// stay on current line, column points past end
		return
	}
	r, size := utf8.DecodeRuneInString(l.input[l.readPosition:])
	l.position = l.readPosition
	l.readPosition += size
	l.ch = r
	if r == '\n' {
		l.line++
		l.column = 0
	} else {
		l.column++
	}
}

func (l *Lexer) peekRune() rune {
	if l.readPosition >= len(l.input) {
		return 0
	}
	r, _ := utf8.DecodeRuneInString(l.input[l.readPosition:])
	return r
}

func (l *Lexer) skipWhitespace() {
	for unicode.IsSpace(l.ch) {
		l.readRune()
	}
}

func (l *Lexer) skipComments() {
	if l.ch == '-' && l.peekRune() == '-' {
		l.consumeLine()
		l.skipWhitespace()
		l.skipComments()
	}
	if l.ch == '/' && l.peekRune() == '*' {
		l.readRune()
		l.readRune()
		l.consumeBlockComment()
		l.skipWhitespace()
		l.skipComments()
	}
}

func (l *Lexer) consumeLine() {
	for l.ch != '\n' && l.ch != 0 {
		l.readRune()
	}
	if l.ch == '\n' {
		l.readRune()
	}
}

func (l *Lexer) consumeBlockComment() {
	for {
		if l.ch == 0 {
			return
		}
		if l.ch == '*' && l.peekRune() == '/' {
			l.readRune()
			l.readRune()
			return
		}
		l.readRune()
	}
}

func (l *Lexer) readIdentifier() string {
	start := l.position
	for isIdentPart(l.ch) {
		l.readRune()
	}
	return l.input[start:l.position]
}

func (l *Lexer) readNumber() string {
	start := l.position
	hasDot := false
	for unicode.IsDigit(l.ch) || (!hasDot && l.ch == '.') {
		if l.ch == '.' {
			hasDot = true
		}
		l.readRune()
	}
	return l.input[start:l.position]
}

func (l *Lexer) readString(quote rune) string {
	var builder strings.Builder
	for {
		l.readRune()
		switch l.ch {
		case quote:
			if l.peekRune() == quote {
				// Escaped quote
				builder.WriteRune(quote)
				l.readRune()
			} else {
				result := builder.String()
				l.readRune()
				return result
			}
		case 0:
			return builder.String()
		default:
			builder.WriteRune(l.ch)
		}
	}
}

func (l *Lexer) readQuotedIdentifier() string {
	var builder strings.Builder
	for {
		l.readRune()
		switch l.ch {
		case '"':
			if l.peekRune() == '"' {
				builder.WriteRune('"')
				l.readRune()
			} else {
				l.readRune()
				return builder.String()
			}
		case 0:
			return builder.String()
		default:
			builder.WriteRune(l.ch)
		}
	}
}

func isIdentStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isIdentPart(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
