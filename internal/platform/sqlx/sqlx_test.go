package sqlx_test

import (
	"context"
	"database/sql"
	"embed"
	"path/filepath"
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

		queryInsert := "INSERT INTO test_items (id, name) VALUES ($1, $2)"
		if driver == "sqlite" {
			queryInsert = "INSERT INTO test_items (id, name) VALUES (?, ?)"
		}

		_, err := db.ExecContext(ctx, queryInsert, "item-1", "First Item")
		if err != nil {
			t.Fatalf("[%s] failed to insert into test_items: %v", driver, err)
		}

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
	db, err := sqlx.Open("sqlite", "")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping sqlite: %v", err)
	}

	pgDB, err := sqlx.Open("postgres", "postgres://127.0.0.1:5432/nonexistent?sslmode=disable")
	if err != nil {
		t.Fatalf("failed to open postgres driver: %v", err)
	}
	defer pgDB.Close()

	_, err = sqlx.Open("unknown_driver", "dsn")
	if err == nil {
		t.Fatal("expected error for unsupported driver, got nil")
	}
}

func TestSQLX_SQLite_PragmasMultiConnection(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pragmas.db")
	db, err := sqlx.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()

	conn1, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire conn1: %v", err)
	}
	defer conn1.Close()

	conn2, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire conn2: %v", err)
	}
	defer conn2.Close()

	for i, conn := range []*sql.Conn{conn1, conn2} {
		var fk int
		if err := conn.QueryRowContext(context.Background(), "PRAGMA foreign_keys;").Scan(&fk); err != nil {
			t.Fatalf("conn%d failed to query foreign_keys: %v", i+1, err)
		}
		if fk != 1 {
			t.Errorf("conn%d expected foreign_keys = 1, got %d", i+1, fk)
		}

		var busyTimeout int
		if err := conn.QueryRowContext(context.Background(), "PRAGMA busy_timeout;").Scan(&busyTimeout); err != nil {
			t.Fatalf("conn%d failed to query busy_timeout: %v", i+1, err)
		}
		if busyTimeout != 5000 {
			t.Errorf("conn%d expected busy_timeout = 5000, got %d", i+1, busyTimeout)
		}
	}
}
