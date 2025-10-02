package logsql

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/VictoriaMetrics/sql-to-logsql/lib/store/tablestore"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/store/viewstore"
)

type showTableRow struct {
	TableName string `json:"table_name"`
	Query     string `json:"query,omitempty"`
}

func buildShowTablesPayload(ts *tablestore.TableStore) (string, error) {
	if ts == nil {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: SHOW TABLES requires configured tables",
		}
	}
	names := ts.ListTables()
	if len(names) == 0 {
		return "", nil
	}
	rows := strings.Builder{}
	for _, name := range names {
		query, _ := ts.GetTableQuery(name)
		row := showTableRow{TableName: name, Query: query}
		payload, err := json.Marshal(row)
		if err != nil {
			return "", &TranslationError{
				Code:    http.StatusInternalServerError,
				Message: "translator: marshal SHOW TABLES payload",
				Err:     err,
			}
		}
		rows.Write(payload)
		rows.WriteByte('\n')
	}
	return rows.String(), nil
}

type showViewRow struct {
	ViewName string `json:"view_name"`
	Query    string `json:"query"`
}

func buildShowViewsPayload(vs *viewstore.ViewStore) (string, error) {
	if vs == nil {
		return "", &TranslationError{
			Code:    http.StatusBadRequest,
			Message: "translator: SHOW VIEWS requires configured views directory",
		}
	}
	names, err := vs.ListViews()
	if err != nil {
		return "", &TranslationError{
			Code:    http.StatusInternalServerError,
			Message: "translator: list views",
			Err:     err,
		}
	}
	if len(names) == 0 {
		return "", nil
	}
	rows := strings.Builder{}
	config, err := vs.ViewDefinitions()
	if err != nil {
		return "", &TranslationError{
			Code:    http.StatusInternalServerError,
			Message: "translator: load view definitions",
			Err:     err,
		}
	}
	for _, name := range names {
		query := config[name]
		row := showViewRow{ViewName: name, Query: query}
		payload, err := json.Marshal(row)
		if err != nil {
			return "", &TranslationError{
				Code:    http.StatusInternalServerError,
				Message: "translator: marshal SHOW VIEWS payload",
				Err:     err,
			}
		}
		rows.Write(payload)
		rows.WriteByte('\n')
	}
	return rows.String(), nil
}
