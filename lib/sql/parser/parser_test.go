package parser_test

import (
	"strings"
	"testing"

	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/ast"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/lexer"
	sqlparser "github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/parser"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/render"
)

func mustParse(t *testing.T, sql string) ast.Statement {
	t.Helper()
	l := lexer.New(sql)
	p := sqlparser.New(l)
	stmt := p.ParseStatement()
	if stmt == nil {
		t.Fatalf("no statement parsed for %q", sql)
	}
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parser returned errors: %v", errs)
	}
	return stmt
}

func mustRender(t *testing.T, node ast.Node) string {
	t.Helper()
	out, err := render.Render(node)
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}
	return out
}

func TestParseSelectStatement(t *testing.T) {
	sql := `SELECT DISTINCT a.id, b.name, COUNT(*) AS total
FROM accounts a
LEFT JOIN balances b ON a.id = b.account_id
WHERE b.amount >= 1000 AND b.status != 'closed'
GROUP BY a.id, b.name
HAVING COUNT(*) > 1
ORDER BY b.name DESC, a.id
LIMIT 10 OFFSET 5;`

	stmt := mustParse(t, sql)
	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if !selectStmt.Distinct {
		t.Fatalf("expected DISTINCT modifier")
	}
	if len(selectStmt.Columns) != 3 {
		t.Fatalf("expected three select items, got %d", len(selectStmt.Columns))
	}

	join, ok := selectStmt.From.(*ast.JoinExpr)
	if !ok {
		t.Fatalf("expected join in FROM clause, got %T", selectStmt.From)
	}
	if join.Type != ast.JoinLeft {
		t.Fatalf("expected LEFT JOIN, got %s", join.Type)
	}

	if selectStmt.Where == nil || selectStmt.Having == nil {
		t.Fatalf("expected WHERE and HAVING clauses to be populated")
	}
	if len(selectStmt.GroupBy) != 2 {
		t.Fatalf("expected 2 GROUP BY expressions, got %d", len(selectStmt.GroupBy))
	}
	if len(selectStmt.OrderBy) != 2 {
		t.Fatalf("expected 2 ORDER BY expressions, got %d", len(selectStmt.OrderBy))
	}
	if selectStmt.Limit == nil || selectStmt.Limit.Count == nil || selectStmt.Limit.Offset == nil {
		t.Fatalf("expected LIMIT and OFFSET to be set")
	}

	rendered := mustRender(t, selectStmt)
	expected := "SELECT DISTINCT a.id, b.name, COUNT(*) AS total FROM accounts AS a LEFT JOIN balances AS b ON (a.id = b.account_id) WHERE ((b.amount >= 1000) AND (b.status != 'closed')) GROUP BY a.id, b.name HAVING (COUNT(*) > 1) ORDER BY b.name DESC, a.id ASC LIMIT 10 OFFSET 5"
	if rendered != expected {
		t.Fatalf("render mismatch:\nexpected: %s\n   actual: %s", expected, rendered)
	}
}

func TestParseInsertValues(t *testing.T) {
	sql := `INSERT INTO accounts (id, name, balance) VALUES (1, 'Alice', 100.0), (2, 'Bob', 250.5)`
	stmt := mustParse(t, sql)
	insertStmt, ok := stmt.(*ast.InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}
	if insertStmt.Table == nil {
		t.Fatalf("expected table information")
	}
	if len(insertStmt.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(insertStmt.Columns))
	}
	if len(insertStmt.Rows) != 2 {
		t.Fatalf("expected 2 value rows, got %d", len(insertStmt.Rows))
	}

	rendered := mustRender(t, insertStmt)
	expected := "INSERT INTO accounts (id, name, balance) VALUES (1, 'Alice', 100.0), (2, 'Bob', 250.5)"
	if rendered != expected {
		t.Fatalf("render mismatch:\nexpected: %s\n   actual: %s", expected, rendered)
	}
}

func TestParseUpdate(t *testing.T) {
	sql := `UPDATE balances SET amount = amount + 10 WHERE account_id = 42`
	stmt := mustParse(t, sql)
	updateStmt, ok := stmt.(*ast.UpdateStatement)
	if !ok {
		t.Fatalf("expected UpdateStatement, got %T", stmt)
	}
	if len(updateStmt.Assignments) != 1 {
		t.Fatalf("expected single assignment, got %d", len(updateStmt.Assignments))
	}
	if updateStmt.Where == nil {
		t.Fatalf("expected WHERE clause")
	}
	rendered := mustRender(t, updateStmt)
	expected := "UPDATE balances SET amount = (amount + 10) WHERE (account_id = 42)"
	if rendered != expected {
		t.Fatalf("render mismatch:\nexpected: %s\n   actual: %s", expected, rendered)
	}
}

func TestParseDelete(t *testing.T) {
	sql := `DELETE FROM balances WHERE account_id IN (SELECT id FROM accounts WHERE status = 'active')`
	stmt := mustParse(t, sql)
	deleteStmt, ok := stmt.(*ast.DeleteStatement)
	if !ok {
		t.Fatalf("expected DeleteStatement, got %T", stmt)
	}
	if deleteStmt.Where == nil {
		t.Fatalf("expected WHERE clause with subquery")
	}
	rendered := mustRender(t, deleteStmt)
	expected := "DELETE FROM balances WHERE account_id IN (SELECT id FROM accounts WHERE (status = 'active'))"
	if rendered != expected {
		t.Fatalf("render mismatch:\nexpected: %s\n   actual: %s", expected, rendered)
	}
}

func TestParseWithAndUnion(t *testing.T) {
	sql := `WITH recent_accounts AS (
    SELECT id, name FROM accounts WHERE created_at > '2024-01-01'
), balances_cte AS (
    SELECT account_id, SUM(amount) AS total FROM transactions GROUP BY account_id
)
SELECT r.id, b.total
FROM recent_accounts AS r
LEFT JOIN balances_cte AS b ON r.id = b.account_id
UNION ALL
SELECT id, 0 FROM archived_accounts`

	stmt := mustParse(t, sql)
	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	if selectStmt.With == nil || len(selectStmt.With.CTEs) != 2 {
		t.Fatalf("expected 2 CTEs, got %v", selectStmt.With)
	}
	if len(selectStmt.SetOps) != 1 {
		t.Fatalf("expected single set operation, got %d", len(selectStmt.SetOps))
	}
	if selectStmt.SetOps[0].Operator != ast.SetOpUnion || !selectStmt.SetOps[0].All {
		t.Fatalf("expected UNION ALL, got %+v", selectStmt.SetOps[0])
	}
	if selectStmt.SetOps[0].Select == nil {
		t.Fatalf("expected right-hand select in set operation")
	}

	rendered := mustRender(t, selectStmt)
	expected := "WITH recent_accounts AS (SELECT id, name FROM accounts WHERE (created_at > '2024-01-01')), balances_cte AS (SELECT account_id, SUM(amount) AS total FROM transactions GROUP BY account_id) SELECT r.id, b.total FROM recent_accounts AS r LEFT JOIN balances_cte AS b ON (r.id = b.account_id) UNION ALL SELECT id, 0 FROM archived_accounts"
	if rendered != expected {
		t.Fatalf("render mismatch:\nexpected: %s\n   actual: %s", expected, rendered)
	}
}

func TestParseAdditionalPredicates(t *testing.T) {
	sql := `SELECT * FROM users WHERE name LIKE 'A%' AND age BETWEEN 18 AND 30 AND deleted IS NULL`
	stmt := mustParse(t, sql)
	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	rendered := mustRender(t, selectStmt)
	expected := "SELECT * FROM users WHERE ((name LIKE 'A%' AND age BETWEEN 18 AND 30) AND deleted IS NULL)"
	if rendered != expected {
		t.Fatalf("render mismatch:\nexpected: %s\n   actual: %s", expected, rendered)
	}

	sql = `SELECT * FROM users WHERE name NOT LIKE '%test%' OR score NOT BETWEEN 1 AND 10 OR archived IS NOT NULL`
	stmt = mustParse(t, sql)
	selectStmt, ok = stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	rendered = mustRender(t, selectStmt)
	expected = "SELECT * FROM users WHERE ((name NOT LIKE '%test%' OR score NOT BETWEEN 1 AND 10) OR archived IS NOT NULL)"
	if rendered != expected {
		t.Fatalf("render mismatch:\nexpected: %s\n   actual: %s", expected, rendered)
	}
}

func TestParseDescribeStatement(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		target ast.DescribeTarget
		parts  []string
	}{
		{
			name:   "describe table simple",
			sql:    "DESCRIBE TABLE logs",
			target: ast.DescribeTable,
			parts:  []string{"logs"},
		},
		{
			name:   "describe view qualified",
			sql:    "DESCRIBE VIEW analytics.daily_errors",
			target: ast.DescribeView,
			parts:  []string{"analytics", "daily_errors"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := mustParse(t, tc.sql)
			describe, ok := stmt.(*ast.DescribeStatement)
			if !ok {
				t.Fatalf("expected DescribeStatement, got %T", stmt)
			}
			if describe.Target != tc.target {
				t.Fatalf("unexpected target: got %s, want %s", describe.Target, tc.target)
			}
			if describe.Name == nil {
				t.Fatalf("expected identifier for describe target")
			}
			if got := describe.Name.Parts; len(got) != len(tc.parts) {
				t.Fatalf("unexpected identifier parts: got %v, want %v", got, tc.parts)
			} else {
				for i := range got {
					if got[i] != tc.parts[i] {
						t.Fatalf("identifier mismatch at %d: got %s, want %s", i, got[i], tc.parts[i])
					}
				}
			}
		})
	}
}

func TestParseShowStatement(t *testing.T) {
	t.Run("show tables", func(t *testing.T) {
		stmt := mustParse(t, "SHOW TABLES")
		if _, ok := stmt.(*ast.ShowTablesStatement); !ok {
			t.Fatalf("expected ShowTablesStatement, got %T", stmt)
		}
	})

	t.Run("show views", func(t *testing.T) {
		stmt := mustParse(t, "SHOW VIEWS")
		if _, ok := stmt.(*ast.ShowViewsStatement); !ok {
			t.Fatalf("expected ShowViewsStatement, got %T", stmt)
		}
	})

	t.Run("invalid target", func(t *testing.T) {
		sql := "SHOW INDEXES"
		l := lexer.New(sql)
		p := sqlparser.New(l)
		stmt := p.ParseStatement()
		if stmt != nil {
			t.Fatalf("expected nil statement, got %T", stmt)
		}
		errs := p.Errors()
		if len(errs) == 0 {
			t.Fatal("expected parse errors for invalid SHOW statement")
		}
		if msg := errs[0].Error(); !strings.Contains(msg, "SHOW expects TABLES or VIEWS") {
			t.Fatalf("unexpected error message: %s", msg)
		}
	})
}

func TestParseCreateView(t *testing.T) {
	t.Run("basic view", func(t *testing.T) {
		sql := `CREATE VIEW active_users AS SELECT id FROM users`
		stmt := mustParse(t, sql)
		view, ok := stmt.(*ast.CreateViewStatement)
		if !ok {
			t.Fatalf("expected CreateViewStatement, got %T", stmt)
		}
		if view.OrReplace {
			t.Fatalf("expected OrReplace=false")
		}
		if view.Materialized {
			t.Fatalf("expected Materialized=false")
		}
		if view.IfNotExists {
			t.Fatalf("expected IfNotExists=false")
		}
		if len(view.Columns) != 0 {
			t.Fatalf("expected no explicit column list, got %d", len(view.Columns))
		}
		if view.Name == nil || strings.Join(view.Name.Parts, ".") != "active_users" {
			t.Fatalf("unexpected view name: %+v", view.Name)
		}
		if view.Select == nil {
			t.Fatalf("expected SELECT statement payload")
		}
		if view.Select.With != nil {
			t.Fatalf("unexpected WITH clause on select")
		}
	})

	t.Run("with modifiers and cte", func(t *testing.T) {
		sql := `CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS reporting.active_users (user_id, total) AS WITH recent AS (SELECT id FROM events WHERE occurred_at > '2024-01-01') SELECT u.id AS user_id, COUNT(*) AS total FROM users AS u`
		stmt := mustParse(t, sql)
		view, ok := stmt.(*ast.CreateViewStatement)
		if !ok {
			t.Fatalf("expected CreateViewStatement, got %T", stmt)
		}
		if !view.OrReplace || !view.Materialized || !view.IfNotExists {
			t.Fatalf("expected all modifiers to be set, got %+v", view)
		}
		if view.Name == nil || strings.Join(view.Name.Parts, ".") != "reporting.active_users" {
			t.Fatalf("unexpected view name: %+v", view.Name)
		}
		if len(view.Columns) != 2 {
			t.Fatalf("expected 2 columns, got %d", len(view.Columns))
		}
		if view.Select == nil {
			t.Fatalf("expected SELECT statement payload")
		}
		if view.Select.With == nil || len(view.Select.With.CTEs) != 1 {
			t.Fatalf("expected single CTE, got %+v", view.Select.With)
		}
	})
}

func TestParseDropView(t *testing.T) {
	t.Run("basic drop", func(t *testing.T) {
		sql := `DROP VIEW reporting.error_logs`
		stmt := mustParse(t, sql)
		drop, ok := stmt.(*ast.DropViewStatement)
		if !ok {
			t.Fatalf("expected DropViewStatement, got %T", stmt)
		}
		if drop.Materialized {
			t.Fatalf("expected Materialized=false")
		}
		if drop.IfExists {
			t.Fatalf("expected IfExists=false")
		}
		if drop.Name == nil || strings.Join(drop.Name.Parts, ".") != "reporting.error_logs" {
			t.Fatalf("unexpected view name: %+v", drop.Name)
		}
	})

	t.Run("materialized if exists", func(t *testing.T) {
		sql := `DROP MATERIALIZED VIEW IF EXISTS analytics.recent`
		stmt := mustParse(t, sql)
		drop, ok := stmt.(*ast.DropViewStatement)
		if !ok {
			t.Fatalf("expected DropViewStatement, got %T", stmt)
		}
		if !drop.Materialized {
			t.Fatalf("expected Materialized=true")
		}
		if !drop.IfExists {
			t.Fatalf("expected IfExists=true")
		}
		if drop.Name == nil || strings.Join(drop.Name.Parts, ".") != "analytics.recent" {
			t.Fatalf("unexpected view name: %+v", drop.Name)
		}
	})
}

func TestParseWindowFunction(t *testing.T) {
	sql := `SELECT SUM(bytes) OVER (PARTITION BY host ORDER BY ts DESC) AS running FROM logs`
	stmt := mustParse(t, sql)
	selectStmt, ok := stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	if len(selectStmt.Columns) != 1 {
		t.Fatalf("expected single select item, got %d", len(selectStmt.Columns))
	}
	fn, ok := selectStmt.Columns[0].Expr.(*ast.FuncCall)
	if !ok {
		t.Fatalf("expected FuncCall, got %T", selectStmt.Columns[0].Expr)
	}
	if fn.Over == nil {
		t.Fatal("expected window specification")
	}
	if len(fn.Over.PartitionBy) != 1 {
		t.Fatalf("expected single partition expression, got %d", len(fn.Over.PartitionBy))
	}
	partIdent, ok := fn.Over.PartitionBy[0].(*ast.Identifier)
	if !ok {
		t.Fatalf("expected partition identifier, got %T", fn.Over.PartitionBy[0])
	}
	if strings.Join(partIdent.Parts, ".") != "host" {
		t.Fatalf("unexpected partition identifier: %+v", partIdent)
	}
	if len(fn.Over.OrderBy) != 1 {
		t.Fatalf("expected single ORDER BY item, got %d", len(fn.Over.OrderBy))
	}
	orderItem := fn.Over.OrderBy[0]
	orderIdent, ok := orderItem.Expr.(*ast.Identifier)
	if !ok {
		t.Fatalf("expected order identifier, got %T", orderItem.Expr)
	}
	if strings.Join(orderIdent.Parts, ".") != "ts" {
		t.Fatalf("unexpected order identifier: %+v", orderIdent)
	}
	if orderItem.Direction != ast.Descending {
		t.Fatalf("expected DESC direction, got %s", orderItem.Direction)
	}
	if selectStmt.Columns[0].Alias != "running" {
		t.Fatalf("unexpected alias %q", selectStmt.Columns[0].Alias)
	}

	sql = `SELECT COUNT(*) OVER () FROM logs`
	stmt = mustParse(t, sql)
	selectStmt, ok = stmt.(*ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	fn, ok = selectStmt.Columns[0].Expr.(*ast.FuncCall)
	if !ok {
		t.Fatalf("expected FuncCall, got %T", selectStmt.Columns[0].Expr)
	}
	if fn.Over == nil {
		t.Fatal("expected window specification")
	}
	if len(fn.Over.PartitionBy) != 0 {
		t.Fatalf("expected no partition expressions, got %d", len(fn.Over.PartitionBy))
	}
	if len(fn.Over.OrderBy) != 0 {
		t.Fatalf("expected no ORDER BY expressions, got %d", len(fn.Over.OrderBy))
	}
}

func TestParseCreateViewRequiresSelect(t *testing.T) {
	sql := `CREATE VIEW v AS DELETE FROM users`
	l := lexer.New(sql)
	p := sqlparser.New(l)
	stmt := p.ParseStatement()
	if _, ok := stmt.(*ast.CreateViewStatement); !ok {
		t.Fatalf("expected CreateViewStatement, got %T", stmt)
	}
	errs := p.Errors()
	if len(errs) == 0 {
		t.Fatalf("expected errors for invalid CREATE VIEW")
	}
	var found bool
	for _, err := range errs {
		if strings.Contains(err.Error(), "requires SELECT") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected error mentioning missing SELECT, got %v", errs)
	}
}

func TestSyntaxErrorPosition(t *testing.T) {
	sql := "SELECT\nFROM accounts"
	l := lexer.New(sql)
	p := sqlparser.New(l)
	_ = p.ParseStatement()

	errs := p.Errors()
	if len(errs) == 0 {
		t.Fatalf("expected parser errors")
	}

	syntaxErr, ok := errs[0].(*sqlparser.SyntaxError)
	if !ok {
		t.Fatalf("expected SyntaxError, got %T", errs[0])
	}
	if syntaxErr.Pos.Line != 2 || syntaxErr.Pos.Column != 1 {
		t.Fatalf("expected error at line 2 column 1, got %+v", syntaxErr.Pos)
	}
	if !strings.Contains(syntaxErr.Error(), "unexpected token FROM") {
		t.Fatalf("expected message to mention unexpected token, got %q", syntaxErr.Error())
	}
}

func TestParseStatementInvalidSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "trailing tokens",
			sql:  "SELECT 1 2",
			want: "unexpected token NUMBER",
		},
		{
			name: "multiple statements",
			sql:  "SELECT 1; SELECT 2",
			want: "unexpected token SELECT",
		},
		{
			name: "incomplete insert",
			sql:  "INSERT INTO logs VALUES",
			want: "expected (, got EOF",
		},
		{
			name: "incomplete update assignment",
			sql:  "UPDATE logs SET",
			want: "expected =, got EOF",
		},
		{
			name: "drop view missing name",
			sql:  "DROP VIEW",
			want: "expected IDENT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.sql)
			p := sqlparser.New(l)
			_ = p.ParseStatement()

			errs := p.Errors()
			if len(errs) == 0 {
				t.Fatalf("expected parse errors, got none")
			}

			if tt.want != "" {
				var matched bool
				for _, err := range errs {
					if strings.Contains(err.Error(), tt.want) {
						matched = true
						break
					}
				}
				if !matched {
					t.Fatalf("expected error containing %q, got %v", tt.want, errs)
				}
			}
		})
	}
}
