package sqlx_test

import (
	"context"
	"database/sql"
	"embed"
	"testing"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
)

//go:embed testdata/migrations/*.sql
var testMigrations embed.FS

// AC-1A-10: Given migrations de exemplo, when a suíte de contrato roda,
// then passa no SQLite; com LGW_TEST_PG_DSN definida, passa igual no Postgres;
// rodar as migrations duas vezes é idempotente.
func TestAC1A10_MigrationsAndContractSuite(t *testing.T) {
	sqlx.RunContractTest(t, testMigrations, "testdata/migrations", func(t *testing.T, db *sql.DB, driver string) {
		ctx := context.Background()

		// Insert into table created by migration 001
		queryInsert := "INSERT INTO test_items (id, name) VALUES ($1, $2)"
		if driver == "sqlite" {
			queryInsert = "INSERT INTO test_items (id, name) VALUES (?, ?)"
		}

		_, err := db.ExecContext(ctx, queryInsert, "item-1", "First Item")
		if err != nil {
			t.Fatalf("[%s] failed to insert into test_items: %v", driver, err)
		}

		// Query table created by migration 001
		var name string
		querySelect := "SELECT name FROM test_items WHERE id = $1"
		if driver == "sqlite" {
			querySelect = "SELECT name FROM test_items WHERE id = ?"
		}
		err = db.QueryRowContext(ctx, querySelect, "item-1").Scan(&name)
		if err != nil {
			t.Fatalf("[%s] failed to select from test_items: %v", driver, err)
		}
		if name != "First Item" {
			t.Fatalf("[%s] expected 'First Item', got %q", driver, name)
		}

		// Insert into table created by migration 002
		queryOrder := "INSERT INTO test_orders (id, item_id, amount_micros) VALUES ($1, $2, $3)"
		if driver == "sqlite" {
			queryOrder = "INSERT INTO test_orders (id, item_id, amount_micros) VALUES (?, ?, ?)"
		}
		_, err = db.ExecContext(ctx, queryOrder, "order-1", "item-1", 10000)
		if err != nil {
			t.Fatalf("[%s] failed to insert into test_orders: %v", driver, err)
		}
	})
}

func TestSQLX_Open(t *testing.T) {
	// SQLite via Open
	db, err := sqlx.Open("sqlite", "")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping sqlite: %v", err)
	}

	// Unsupported driver
	_, err = sqlx.Open("unknown_driver", "dsn")
	if err == nil {
		t.Fatal("expected error for unsupported driver, got nil")
	}
}
