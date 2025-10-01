package logsql

import (
	"fmt"
	"net/http"

	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/sql/ast"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/store"
)

type StatementType string

const (
	StatementTypeSelect     StatementType = "select"
	StatementTypeDescribe   StatementType = "describe"
	StatementTypeCreateView StatementType = "create_view"
	StatementTypeDropView   StatementType = "drop_view"
	StatementTypeShowTables StatementType = "show_tables"
	StatementTypeShowViews  StatementType = "show_views"
)

type StatementInfo struct {
	Kind   StatementType
	LogsQL string
	Data   string
}

func GetStatementInfo(
	stmt ast.Statement,
	sp *store.Provider,
) (*StatementInfo, error) {
	if stmt == nil {
		return nil, fmt.Errorf("translator: nil statement")
	}

	switch s := stmt.(type) {
	case *ast.DescribeStatement:
		query, err := GetDescribeStatementLogsQL(s, sp)
		if err != nil {
			return nil, err
		}
		return &StatementInfo{LogsQL: query, Kind: StatementTypeDescribe}, nil
	case *ast.CreateViewStatement:
		query, err := ExecuteCreateStatement(s, sp)
		if err != nil {
			return nil, err
		}
		return &StatementInfo{LogsQL: query, Kind: StatementTypeCreateView}, nil
	case *ast.DropViewStatement:
		query, err := ExecuteDropStatement(s, sp)
		if err != nil {
			return nil, err
		}
		return &StatementInfo{LogsQL: query, Kind: StatementTypeDropView}, nil
	case *ast.ShowTablesStatement:
		payload, err := buildShowTablesPayload(sp.TableStore())
		if err != nil {
			return nil, err
		}
		return &StatementInfo{Kind: StatementTypeShowTables, Data: payload}, nil
	case *ast.ShowViewsStatement:
		payload, err := buildShowViewsPayload(sp.ViewStore())
		if err != nil {
			return nil, err
		}
		return &StatementInfo{Kind: StatementTypeShowViews, Data: payload}, nil
	case *ast.SelectStatement:
		query, err := TranslateSelectStatementToLogsQL(stmt, sp)
		if err != nil {
			return nil, err
		}
		return &StatementInfo{LogsQL: query, Kind: StatementTypeSelect}, nil
	default:
		return nil, &TranslationError{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("translator: unsupported statement %T", s),
		}
	}
}
