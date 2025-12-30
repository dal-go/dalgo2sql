package dalgo2sql

import (
	"database/sql"
	"strings"
	"testing"
)

func closeDatabase(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	if err := sqlDB.Close(); err != nil {
		if strings.Contains(err.Error(), "call to database Close was not expected") ||
			strings.Contains(err.Error(), "call to database Close, was not expected") {
			return
		}
		t.Errorf("failed to close database: %v", err)
	}
}
