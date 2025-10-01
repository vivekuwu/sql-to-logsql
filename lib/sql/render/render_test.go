package render

import (
	"strings"
	"testing"

	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/ast"
)

func TestRenderSelectWithCTEAndUnion(t *testing.T) {
	stmt := &ast.SelectStatement{
		With: &ast.WithClause{
			CTEs: []ast.CommonTableExpression{
				{
					Name: &ast.Identifier{Parts: []string{"recent"}},
					Select: &ast.SelectStatement{
						Columns: []ast.SelectItem{{Expr: &ast.Identifier{Parts: []string{"id"}}}},
						From:    &ast.TableName{Name: &ast.Identifier{Parts: []string{"accounts"}}},
					},
				},
			},
		},
		Columns: []ast.SelectItem{{Expr: &ast.Identifier{Parts: []string{"recent", "id"}}}},
		From:    &ast.TableName{Name: &ast.Identifier{Parts: []string{"recent"}}, Alias: "r"},
		Where: &ast.BinaryExpr{
			Left:     &ast.Identifier{Parts: []string{"r", "active"}},
			Operator: "=",
			Right:    &ast.BooleanLiteral{Value: true},
		},
		OrderBy: []ast.OrderItem{{
			Expr:      &ast.Identifier{Parts: []string{"recent", "id"}},
			Direction: ast.Descending,
		}},
		SetOps: []ast.SetOperation{{
			Operator: ast.SetOpUnion,
			All:      true,
			Select: &ast.SelectStatement{
				Columns: []ast.SelectItem{{Expr: &ast.NumericLiteral{Value: "0"}}},
			},
		}},
	}

	out, err := Render(stmt)
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}

	expected := "WITH recent AS (SELECT id FROM accounts) SELECT recent.id FROM recent AS r WHERE (r.active = TRUE) ORDER BY recent.id DESC UNION ALL SELECT 0"
	if out != expected {
		t.Fatalf("unexpected render output:\nexpected: %s\nactual:   %s", expected, out)
	}
}

func TestRenderInsertValues(t *testing.T) {
	stmt := &ast.InsertStatement{
		Table: &ast.TableName{Name: &ast.Identifier{Parts: []string{"accounts"}}},
		Columns: []*ast.Identifier{
			{Parts: []string{"id"}},
			{Parts: []string{"name"}},
		},
		Rows: [][]ast.Expr{
			{
				&ast.NumericLiteral{Value: "1"},
				&ast.StringLiteral{Value: "Alice"},
			},
			{
				&ast.NumericLiteral{Value: "2"},
				&ast.StringLiteral{Value: "Bob"},
			},
		},
	}

	out, err := Render(stmt)
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}

	expected := "INSERT INTO accounts (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
	if out != expected {
		t.Fatalf("unexpected render output:\nexpected: %s\nactual:   %s", expected, out)
	}
}

func TestRenderNilSetOperandError(t *testing.T) {
	stmt := &ast.SelectStatement{
		Columns: []ast.SelectItem{{Expr: &ast.NumericLiteral{Value: "1"}}},
		SetOps: []ast.SetOperation{{
			Operator: ast.SetOpUnion,
			Select:   nil,
		}},
	}

	_, err := Render(stmt)
	if err == nil {
		t.Fatalf("expected error for nil set operand")
	}

	if !strings.Contains(err.Error(), "nil set operand") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestRenderCreateView(t *testing.T) {
	selectStmt := &ast.SelectStatement{
		With: &ast.WithClause{
			CTEs: []ast.CommonTableExpression{
				{
					Name: &ast.Identifier{Parts: []string{"recent"}},
					Select: &ast.SelectStatement{
						Columns: []ast.SelectItem{{Expr: &ast.Identifier{Parts: []string{"id"}}}},
						From:    &ast.TableName{Name: &ast.Identifier{Parts: []string{"events"}}},
						Where: &ast.BinaryExpr{
							Left:     &ast.Identifier{Parts: []string{"occurred_at"}},
							Operator: ">",
							Right:    &ast.StringLiteral{Value: "2024-01-01"},
						},
					},
				},
			},
		},
		Columns: []ast.SelectItem{
			{Expr: &ast.Identifier{Parts: []string{"u", "id"}}, Alias: "user_id"},
			{Expr: &ast.FuncCall{Name: ast.Identifier{Parts: []string{"COUNT"}}, Args: []ast.Expr{&ast.StarExpr{}}}, Alias: "total"},
		},
		From: &ast.TableName{Name: &ast.Identifier{Parts: []string{"users"}}, Alias: "u"},
		Where: &ast.BinaryExpr{
			Left:     &ast.Identifier{Parts: []string{"u", "active"}},
			Operator: "=",
			Right:    &ast.BooleanLiteral{Value: true},
		},
	}

	view := &ast.CreateViewStatement{
		OrReplace:    true,
		Materialized: true,
		IfNotExists:  true,
		Name:         &ast.Identifier{Parts: []string{"reporting", "active_users"}},
		Columns: []*ast.Identifier{
			{Parts: []string{"user_id"}},
			{Parts: []string{"total"}},
		},
		Select: selectStmt,
	}

	out, err := Render(view)
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}

	expected := "CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS reporting.active_users (user_id, total) AS WITH recent AS (SELECT id FROM events WHERE (occurred_at > '2024-01-01')) SELECT u.id AS user_id, COUNT(*) AS total FROM users AS u WHERE (u.active = TRUE)"
	if out != expected {
		t.Fatalf("unexpected render output:\nexpected: %s\nactual:   %s", expected, out)
	}
}

func TestRenderCreateViewMissingSelect(t *testing.T) {
	view := &ast.CreateViewStatement{Name: &ast.Identifier{Parts: []string{"v"}}}
	_, err := Render(view)
	if err == nil {
		t.Fatalf("expected error when CREATE VIEW has nil select")
	}
	if !strings.Contains(err.Error(), "missing select") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
