package logsql

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/ast"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/store"
)

func ExecuteDropStatement(stmt ast.Statement, sp *store.Provider) (string, error) {
	if stmt == nil {
		return "", fmt.Errorf("translator: nil statement")
	}
	t := &dropExecuteVisitor{sp: sp}
	stmt.Accept(t)
	if t.err != nil {
		return "", t.err
	}
	return "", nil
}

type dropExecuteVisitor struct {
	err error
	sp  *store.Provider
}

func (v *dropExecuteVisitor) Visit(node ast.Node) ast.Visitor {
	if node == nil || v.err != nil {
		return v
	}
	switch n := node.(type) {
	case *ast.DropViewStatement:
		v.err = v.translateDropView(n)
	default:
		v.err = &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported root node %T", n),
		}
	}
	return nil
}

func (v *dropExecuteVisitor) translateDropView(stmt *ast.DropViewStatement) error {
	if stmt == nil {
		return fmt.Errorf("translator: DROP VIEW statement is nil")
	}
	if v.sp.ViewStore() == nil {
		return &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: DROP VIEW requires configured views directory",
		}
	}
	if stmt.Materialized {
		name := "view"
		if stmt.Name != nil && len(stmt.Name.Parts) > 0 {
			name = strings.Join(stmt.Name.Parts, ".")
		}
		return &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: DROP MATERIALIZED VIEW %s is not supported", name),
		}
	}
	if stmt.Name == nil || len(stmt.Name.Parts) == 0 {
		return &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: DROP VIEW missing name",
		}
	}
	return v.sp.ViewStore().Remove(stmt.Name.Parts, stmt.IfExists)
}
