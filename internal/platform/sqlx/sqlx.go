package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	// Register pgx driver for database/sql PostgreSQL support.
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

func buildSQLiteDSN(dsn string) string {
	if dsn == "" {
		dsn = "file::memory:?cache=shared"
	}
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + "_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)"
}

// OpenSQLite opens a SQLite database with WAL journal mode, 5s busy timeout, and foreign keys enabled.
func OpenSQLite(dsn string) (*sql.DB, error) {
	fullDSN := buildSQLiteDSN(dsn)
	db, err := sql.Open("sqlite", fullDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite: %w", err)
	}
	return db, nil
}

// OpenPostgres opens a PostgreSQL database using pgx stdlib driver.
func OpenPostgres(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return db, nil
}

// Open opens a database based on the driver name ("sqlite", "sqlite3", "postgres", "pgx").
func Open(driver, dsn string) (*sql.DB, error) {
	switch strings.ToLower(driver) {
	case "sqlite", "sqlite3":
		return OpenSQLite(dsn)
	case "postgres", "postgresql", "pgx":
		return OpenPostgres(dsn)
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driver)
	}
}

func listMigrationFiles(fsys fs.FS, dir string) ([]string, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations dir %s: %w", dir, err)
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)
	return files, nil
}

func getAppliedMigrations(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return applied, nil
}

func applySingleMigration(ctx context.Context, db *sql.DB, fsys fs.FS, dir, filename string) error {
	filePath := filepath.ToSlash(filepath.Join(dir, filename))
	content, err := fs.ReadFile(fsys, filePath)
	if err != nil {
		return fmt.Errorf("failed to read migration file %s: %w", filename, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx for %s: %w", filename, err)
	}

	if _, err := tx.ExecContext(ctx, string(content)); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to apply migration %s: %w", filename, err)
	}

	insertSQL := "INSERT INTO schema_migrations (version, applied_at) VALUES ($1, $2)"
	if _, err := tx.ExecContext(ctx, insertSQL, filename, time.Now().UTC()); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("failed to record migration %s: %w", filename, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migration %s: %w", filename, err)
	}
	return nil
}

// Migrate executes all .sql migration files found in fsys at dir in alphabetical order.
// Executing Migrate twice is idempotent.
func Migrate(ctx context.Context, db *sql.DB, fsys fs.FS, dir string) error {
	createTableSQL := `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL
);`
	if _, err := db.ExecContext(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	files, err := listMigrationFiles(fsys, dir)
	if err != nil {
		return err
	}

	applied, err := getAppliedMigrations(ctx, db)
	if err != nil {
		return err
	}

	for _, filename := range files {
		if applied[filename] {
			continue
		}
		if err := applySingleMigration(ctx, db, fsys, dir, filename); err != nil {
			return err
		}
	}

	return nil
}

// RunContractTest executes testFn against SQLite and, if LGW_TEST_PG_DSN is set, against PostgreSQL.
// It runs Migrate twice before calling testFn to guarantee idempotency.
func RunContractTest(t *testing.T, fsys fs.FS, migrationDir string, testFn func(t *testing.T, db *sql.DB, driver string)) {
	t.Helper()

	t.Run("SQLite", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "contract.db")
		db, err := OpenSQLite(dbPath)
		if err != nil {
			t.Fatalf("failed to open sqlite: %v", err)
		}
		defer db.Close()

		ctx := context.Background()
		if err := Migrate(ctx, db, fsys, migrationDir); err != nil {
			t.Fatalf("migration run 1 failed: %v", err)
		}
		if err := Migrate(ctx, db, fsys, migrationDir); err != nil {
			t.Fatalf("migration run 2 (idempotency) failed: %v", err)
		}

		testFn(t, db, "sqlite")
	})

	pgDSN := os.Getenv("LGW_TEST_PG_DSN")
	if pgDSN == "" {
		return
	}

	t.Run("PostgreSQL", func(t *testing.T) {
		db, err := OpenPostgres(pgDSN)
		if err != nil {
			t.Fatalf("failed to open postgres: %v", err)
		}
		defer db.Close()

		ctx := context.Background()
		if err := Migrate(ctx, db, fsys, migrationDir); err != nil {
			t.Fatalf("pg migration run 1 failed: %v", err)
		}
		if err := Migrate(ctx, db, fsys, migrationDir); err != nil {
			t.Fatalf("pg migration run 2 (idempotency) failed: %v", err)
		}

		testFn(t, db, "postgres")
	})
}
