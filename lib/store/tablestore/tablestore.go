package tablestore

import (
	"fmt"
	"sort"
	"strings"
)

type TableStore struct {
	tables map[string]string
}

func NewTableStore(tables map[string]string) (*TableStore, error) {
	tables, err := normalizeTableMap(tables)
	if err != nil {
		return nil, fmt.Errorf("normalize table map: %w", err)
	}
	return &TableStore{
		tables: tables,
	}, nil
}

func (s *TableStore) GetTableQuery(name string) (string, bool) {
	expr, ok := s.tables[name]
	return expr, ok
}

func (s *TableStore) ListTables() []string {
	tables := make([]string, 0, len(s.tables))
	for tbl := range s.tables {
		tables = append(tables, tbl)
	}
	sort.Strings(tables)
	return tables
}

func normalizeTableMap(src map[string]string) (map[string]string, error) {
	dst := make(map[string]string, len(src))
	if len(src) == 0 {
		return dst, nil
	}
	for name, expr := range src {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			return nil, fmt.Errorf("translator: table name cannot be empty")
		}
		if _, exists := dst[key]; exists {
			return nil, fmt.Errorf("translator: duplicate table name %q", key)
		}
		dst[key] = strings.TrimSpace(expr)
	}
	return dst, nil
}
