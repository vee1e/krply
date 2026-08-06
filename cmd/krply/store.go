package main

import (
	"github.com/krply/krply/internal/storage"
)

// openStore opens the local journal. The special path ":memory:" yields an
// ephemeral in-memory store.
func openStore(path string) (storage.Store, error) {
	return storage.NewSQLiteStore(path)
}

// closeStore releases a store opened with openStore.
func closeStore(s storage.Store) {
	if s != nil {
		_ = s.Close()
	}
}
