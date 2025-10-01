package logsql

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/ast"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/store"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/store/tablestore"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/store/viewstore"
)

func GetDescribeStatementLogsQL(stmt *ast.DescribeStatement, sp *store.Provider) (string, error) {
	if stmt.Name == nil || len(stmt.Name.Parts) == 0 {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: DESCRIBE requires a target name",
		}
	}

	switch stmt.Target {
	case ast.DescribeTable:
		return getDescribeStatementTableLogsQL(stmt.Name.Parts, sp.TableStore())
	case ast.DescribeView:
		return getDescribeStatementViewLogsQL(stmt.Name.Parts, sp.ViewStore())
	default:
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported DESCRIBE target %q", stmt.Target),
		}
	}
}

func getDescribeStatementTableLogsQL(parts []string, tableStore *tablestore.TableStore) (string, error) {
	name := parts[len(parts)-1]
	key := strings.ToLower(name)
	expr, ok := tableStore.GetTableQuery(key)
	if !ok {
		available := tableStore.ListTables()
		return "", &TranslationError{
			Code:    http.StatusNotFound,
			Message: fmt.Sprintf("translator: table %q is not configured (available: %s)", strings.Join(parts, "."), strings.Join(available, ", ")),
		}
	}
	return expr, nil
}

func getDescribeStatementViewLogsQL(parts []string, store *viewstore.ViewStore) (string, error) {
	if store == nil {
		return "", fmt.Errorf("translator: DESCRIBE VIEW requires configured views directory")
	}
	query, display, found, err := store.Load(parts)
	if err != nil {
		return "", err
	}
	if !found {
		return "", &TranslationError{
			Code:    http.StatusNotFound,
			Message: fmt.Sprintf("translator: view %s not found", display),
		}
	}
	return query, nil
}
