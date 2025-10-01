package store

import (
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/store/tablestore"
	"github.com/VictoriaMetrics-Community/sql-to-logsql/lib/store/viewstore"
)

type Provider struct {
	tableStore *tablestore.TableStore
	viewStore  *viewstore.ViewStore
}

func NewStoreProvider(tableStore *tablestore.TableStore, viewStore *viewstore.ViewStore) *Provider {
	return &Provider{
		tableStore: tableStore,
		viewStore:  viewStore,
	}
}

func (s *Provider) TableStore() *tablestore.TableStore {
	return s.tableStore
}

func (s *Provider) ViewStore() *viewstore.ViewStore {
	return s.viewStore
}
