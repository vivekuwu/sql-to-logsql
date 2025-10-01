package logsql

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/ast"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/store"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/store/viewstore"
)

func ExecuteCreateStatement(stmt ast.Statement, sp *store.Provider) (string, error) {
	if stmt == nil {
		return "", fmt.Errorf("translator: nil statement")
	}
	t := &createExecuteVisitor{sp: sp}
	stmt.Accept(t)
	if t.err != nil {
		return "", t.err
	}
	return t.result, nil
}

type createExecuteVisitor struct {
	result string
	err    error
	sp     *store.Provider
}

func (v *createExecuteVisitor) Visit(node ast.Node) ast.Visitor {
	if node == nil || v.err != nil {
		return v
	}
	switch n := node.(type) {
	case *ast.CreateViewStatement:
		v.err = v.translateCreateView(n)
	default:
		v.err = &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported root node %T", n),
		}
	}
	return nil
}

func (v *createExecuteVisitor) translateCreateView(stmt *ast.CreateViewStatement) error {
	if stmt == nil {
		return fmt.Errorf("translator: CREATE VIEW statement is nil")
	}
	if v.sp.ViewStore() == nil {
		return fmt.Errorf("translator: CREATE VIEW requires configured views directory")
	}
	if stmt.Materialized {
		name := "view"
		if stmt.Name != nil && len(stmt.Name.Parts) > 0 {
			name = strings.Join(stmt.Name.Parts, ".")
		}
		return &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: MATERIALIZED VIEW %s is not supported", name),
		}
	}
	if stmt.Name == nil || len(stmt.Name.Parts) == 0 {
		return &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: CREATE VIEW missing name",
		}
	}
	if stmt.Select == nil {
		return &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: CREATE VIEW %s missing SELECT", strings.Join(stmt.Name.Parts, ".")),
		}
	}
	nestedCtx := translationContext{sp: v.sp}
	query, err := translateSelectStatementToLogsQLWithContext(stmt.Select, nestedCtx)
	if err != nil {
		return fmt.Errorf("translator: failed to translate SELECT for view %s: %w", strings.Join(stmt.Name.Parts, "."), err)
	}
	_, err = v.sp.ViewStore().Save(stmt.Name.Parts, query, viewstore.ViewOptions{OrReplace: stmt.OrReplace, IfNotExists: stmt.IfNotExists})
	v.result = query
	return err
}
