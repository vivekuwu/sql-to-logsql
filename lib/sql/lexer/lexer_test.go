package lexer

import (
	"testing"

	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/token"
)

func TestNextTokenSelect(t *testing.T) {
	input := `SELECT DISTINCT a.id, b.name
FROM accounts AS a
INNER JOIN balances b ON a.id = b.account_id
WHERE b.amount >= 1000.50 AND b.status != 'closed'
GROUP BY a.id, b.name
HAVING COUNT(*) > 1
ORDER BY b.updated_at DESC;
`

	expected := []token.Token{
		{Type: token.SELECT, Literal: "SELECT"},
		{Type: token.DISTINCT, Literal: "DISTINCT"},
		{Type: token.IDENT, Literal: "a"},
		{Type: token.DOT, Literal: "."},
		{Type: token.IDENT, Literal: "id"},
		{Type: token.COMMA, Literal: ","},
		{Type: token.IDENT, Literal: "b"},
		{Type: token.DOT, Literal: "."},
		{Type: token.IDENT, Literal: "name"},
		{Type: token.FROM, Literal: "FROM"},
		{Type: token.IDENT, Literal: "accounts"},
		{Type: token.AS, Literal: "AS"},
		{Type: token.IDENT, Literal: "a"},
		{Type: token.INNER, Literal: "INNER"},
		{Type: token.JOIN, Literal: "JOIN"},
		{Type: token.IDENT, Literal: "balances"},
		{Type: token.IDENT, Literal: "b"},
		{Type: token.ON, Literal: "ON"},
		{Type: token.IDENT, Literal: "a"},
		{Type: token.DOT, Literal: "."},
		{Type: token.IDENT, Literal: "id"},
		{Type: token.EQ, Literal: "="},
		{Type: token.IDENT, Literal: "b"},
		{Type: token.DOT, Literal: "."},
		{Type: token.IDENT, Literal: "account_id"},
		{Type: token.WHERE, Literal: "WHERE"},
		{Type: token.IDENT, Literal: "b"},
		{Type: token.DOT, Literal: "."},
		{Type: token.IDENT, Literal: "amount"},
		{Type: token.GTE, Literal: ">="},
		{Type: token.NUMBER, Literal: "1000.50"},
		{Type: token.AND, Literal: "AND"},
		{Type: token.IDENT, Literal: "b"},
		{Type: token.DOT, Literal: "."},
		{Type: token.IDENT, Literal: "status"},
		{Type: token.NEQ, Literal: "!="},
		{Type: token.STRING, Literal: "closed"},
		{Type: token.GROUP, Literal: "GROUP"},
		{Type: token.BY, Literal: "BY"},
		{Type: token.IDENT, Literal: "a"},
		{Type: token.DOT, Literal: "."},
		{Type: token.IDENT, Literal: "id"},
		{Type: token.COMMA, Literal: ","},
		{Type: token.IDENT, Literal: "b"},
		{Type: token.DOT, Literal: "."},
		{Type: token.IDENT, Literal: "name"},
		{Type: token.HAVING, Literal: "HAVING"},
		{Type: token.IDENT, Literal: "COUNT"},
		{Type: token.LPAREN, Literal: "("},
		{Type: token.STAR, Literal: "*"},
		{Type: token.RPAREN, Literal: ")"},
		{Type: token.GT, Literal: ">"},
		{Type: token.NUMBER, Literal: "1"},
		{Type: token.ORDER, Literal: "ORDER"},
		{Type: token.BY, Literal: "BY"},
		{Type: token.IDENT, Literal: "b"},
		{Type: token.DOT, Literal: "."},
		{Type: token.IDENT, Literal: "updated_at"},
		{Type: token.DESC, Literal: "DESC"},
		{Type: token.SEMICOLON, Literal: ";"},
		{Type: token.EOF, Literal: ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.Type || tok.Literal != exp.Literal {
			t.Fatalf("token[%d] - expected %#v, got %#v", i, exp, tok)
		}
	}
}

func TestNextTokenLiterals(t *testing.T) {
	input := `INSERT INTO foo VALUES (1, 'two', ?, "WeirdName");`

	expected := []token.Type{
		token.INSERT,
		token.INTO,
		token.IDENT,
		token.VALUES,
		token.LPAREN,
		token.NUMBER,
		token.COMMA,
		token.STRING,
		token.COMMA,
		token.PLACEHOLDER,
		token.COMMA,
		token.IDENT,
		token.RPAREN,
		token.SEMICOLON,
		token.EOF,
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp {
			t.Fatalf("token[%d] - expected %s, got %s", i, exp, tok.Type)
		}
	}
}

func TestNextTokenCreateView(t *testing.T) {
	input := `CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS reporting.active_users (id, name) AS SELECT id, name FROM users WHERE active = TRUE;`

	expected := []token.Type{
		token.CREATE,
		token.OR,
		token.REPLACE,
		token.MATERIALIZED,
		token.VIEW,
		token.IF,
		token.NOT,
		token.EXISTS,
		token.IDENT,
		token.DOT,
		token.IDENT,
		token.LPAREN,
		token.IDENT,
		token.COMMA,
		token.IDENT,
		token.RPAREN,
		token.AS,
		token.SELECT,
		token.IDENT,
		token.COMMA,
		token.IDENT,
		token.FROM,
		token.IDENT,
		token.WHERE,
		token.IDENT,
		token.EQ,
		token.TRUE,
		token.SEMICOLON,
		token.EOF,
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp {
			t.Fatalf("token[%d] - expected %s, got %s", i, exp, tok.Type)
		}
	}
}

func TestNextTokenDropView(t *testing.T) {
	input := `DROP VIEW IF EXISTS reporting.error_logs;`

	expected := []token.Type{
		token.DROP,
		token.VIEW,
		token.IF,
		token.EXISTS,
		token.IDENT,
		token.DOT,
		token.IDENT,
		token.SEMICOLON,
		token.EOF,
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp {
			t.Fatalf("token[%d] - expected %s, got %s", i, exp, tok.Type)
		}
	}
}

func TestNextTokenShow(t *testing.T) {
	input := `SHOW TABLES; SHOW VIEWS`

	expected := []token.Type{
		token.SHOW,
		token.TABLES,
		token.SEMICOLON,
		token.SHOW,
		token.VIEWS,
		token.EOF,
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp {
			t.Fatalf("token[%d] - expected %s, got %s", i, exp, tok.Type)
		}
	}
}

func TestNextTokenWindow(t *testing.T) {
	input := `SELECT SUM(value) OVER (PARTITION BY host ORDER BY ts) FROM logs`

	expected := []token.Type{
		token.SELECT,
		token.IDENT,
		token.LPAREN,
		token.IDENT,
		token.RPAREN,
		token.OVER,
		token.LPAREN,
		token.PARTITION,
		token.BY,
		token.IDENT,
		token.ORDER,
		token.BY,
		token.IDENT,
		token.RPAREN,
		token.FROM,
		token.IDENT,
		token.EOF,
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp {
			t.Fatalf("token[%d] - expected %s, got %s", i, exp, tok.Type)
		}
	}
}
