package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
)

var (
	ErrNotFound = errors.New("not found")
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate runs migrations for the usage store.
func Migrate(ctx context.Context, db *sql.DB) error {
	return sqlx.Migrate(ctx, db, migrationsFS, "migrations")
}

type Store struct {
	db       *sql.DB
	isSQLite bool
	mu       sync.Mutex
}

func New(db *sql.DB) *Store {
	driverName := fmt.Sprintf("%T", db.Driver())
	isSQLite := strings.Contains(strings.ToLower(driverName), "sqlite")
	return &Store{
		db:       db,
		isSQLite: isSQLite,
	}
}

func (s *Store) DB() *sql.DB {
	return s.db
}

type UsageRecord struct {
	RequestID        string    `json:"request_id"`
	KeyID            string    `json:"key_id"`
	Project          string    `json:"project"`
	ModelRequested   string    `json:"model_requested"`
	ModelUsed        string    `json:"model_used"`
	Provider         string    `json:"provider"`
	Source           string    `json:"source"`
	CostKind         string    `json:"cost_kind"`
	InputTokens      int64     `json:"input_tokens"`
	OutputTokens     int64     `json:"output_tokens"`
	CacheReadTokens  int64     `json:"cache_read_tokens"`
	CacheWriteTokens int64     `json:"cache_write_tokens"`
	CostMicros       int64     `json:"cost_micros"`
	LatencyMs        int64     `json:"latency_ms"`
	Status           string    `json:"status"`
	CacheHit         bool      `json:"cache_hit"`
	FallbackFrom     string    `json:"fallback_from"`
	CreatedAt        time.Time `json:"created_at"`
}

type DailyAggregate struct {
	Date             string    `json:"date"`
	KeyID            string    `json:"key_id"`
	Model            string    `json:"model"`
	Provider         string    `json:"provider"`
	Source           string    `json:"source"`
	CostKind         string    `json:"cost_kind"`
	Requests         int64     `json:"requests"`
	InputTokens      int64     `json:"input_tokens"`
	OutputTokens     int64     `json:"output_tokens"`
	CacheReadTokens  int64     `json:"cache_read_tokens"`
	CacheWriteTokens int64     `json:"cache_write_tokens"`
	CostMicros       int64     `json:"cost_micros"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type AuditEvent struct {
	ID          string    `json:"id"`
	EventType   string    `json:"event_type"`
	Subject     string    `json:"subject"`
	PayloadJSON string    `json:"payload_json"`
	CreatedAt   time.Time `json:"created_at"`
}

type ImportCursor struct {
	Source     string    `json:"source"`
	FilePath   string    `json:"file_path"`
	ByteOffset int64     `json:"byte_offset"`
	LastLine   int64     `json:"last_line"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type ImportRun struct {
	ID              string     `json:"id"`
	Source          string     `json:"source"`
	Status          string     `json:"status"`
	LinesRead       int64      `json:"lines_read"`
	RecordsImported int64      `json:"records_imported"`
	ErrorsCount     int64      `json:"errors_count"`
	ErrorDetails    string     `json:"error_details"`
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
}

type Subscription struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	Provider          string    `json:"provider"`
	MonthlyCostMicros int64     `json:"monthly_cost_micros"`
	RenewalDay        int       `json:"renewal_day"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type UsageFilter struct {
	StartDate string
	EndDate   string
	KeyID     string
	Model     string
	Provider  string
	Source    string
	CostKind  string
	Limit     int
	Offset    int
}

func (s *Store) lockSQLite() func() {
	if s.isSQLite {
		s.mu.Lock()
		return s.mu.Unlock
	}
	return func() {}
}

func (s *Store) RecordUsage(ctx context.Context, req UsageRecord) (bool, error) {
	unlock := s.lockSQLite()
	defer unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	inserted, err := s.processRecordTx(ctx, tx, req)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit tx: %w", err)
	}
	return inserted, nil
}

func (s *Store) RecordUsageBatch(ctx context.Context, records []UsageRecord) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	unlock := s.lockSQLite()
	defer unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	insertedCount := 0
	for _, req := range records {
		inserted, err := s.processRecordTx(ctx, tx, req)
		if err != nil {
			return 0, err
		}
		if inserted {
			insertedCount++
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit tx: %w", err)
	}
	return insertedCount, nil
}

func (s *Store) updateOrgCostDiffTx(ctx context.Context, tx *sql.Tx, dateStr string, req UsageRecord, diff int64) error {
	_, err := tx.ExecContext(ctx, `UPDATE usage_requests SET cost_micros = $1, input_tokens = $2, output_tokens = $3 WHERE request_id = $4`,
		req.CostMicros, req.InputTokens, req.OutputTokens, req.RequestID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `UPDATE daily_aggregates SET cost_micros = daily_aggregates.cost_micros + $1, updated_at = $2
		WHERE date = $3 AND model = $4 AND provider = $5 AND source = $6 AND cost_kind = $7`,
		diff, now, dateStr, req.ModelUsed, req.Provider, req.Source, req.CostKind)
	return err
}

func (s *Store) processOrgSnapshotTx(ctx context.Context, tx *sql.Tx, dateStr string, req UsageRecord) (bool, error) {
	var existingCost int64
	err := tx.QueryRowContext(ctx, "SELECT cost_micros FROM usage_requests WHERE request_id = $1", req.RequestID).Scan(&existingCost)
	if errors.Is(err, sql.ErrNoRows) {
		return s.processStandardRecordTx(ctx, tx, dateStr, req)
	}
	if err != nil {
		return false, err
	}

	diff := req.CostMicros - existingCost
	if diff == 0 {
		return false, nil
	}

	if err := s.updateOrgCostDiffTx(ctx, tx, dateStr, req, diff); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) processStandardRecordTx(ctx context.Context, tx *sql.Tx, dateStr string, req UsageRecord) (bool, error) {
	inserted, err := s.insertUsageTx(ctx, tx, req)
	if err != nil || !inserted {
		return false, err
	}
	if err := s.upsertDailyAggregateTx(ctx, tx, dateStr, req); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) processRecordTx(ctx context.Context, tx *sql.Tx, req UsageRecord) (bool, error) {
	dateStr := req.CreatedAt.UTC().Format("2006-01-02")
	if req.Source == "anthropic-org" || req.Source == "openai-org" {
		return s.processOrgSnapshotTx(ctx, tx, dateStr, req)
	}
	return s.processStandardRecordTx(ctx, tx, dateStr, req)
}

func (s *Store) insertUsageTx(ctx context.Context, tx *sql.Tx, req UsageRecord) (bool, error) {
	query := `
INSERT INTO usage_requests (
    request_id, key_id, project, model_requested, model_used, provider, source, cost_kind,
    input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
    cost_micros, latency_ms, status, cache_hit, fallback_from, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
ON CONFLICT (request_id) DO NOTHING`

	res, err := tx.ExecContext(ctx, query,
		req.RequestID, req.KeyID, req.Project, req.ModelRequested, req.ModelUsed,
		req.Provider, req.Source, req.CostKind, req.InputTokens, req.OutputTokens,
		req.CacheReadTokens, req.CacheWriteTokens, req.CostMicros, req.LatencyMs,
		req.Status, req.CacheHit, req.FallbackFrom, req.CreatedAt.UTC(),
	)
	if err != nil {
		return false, fmt.Errorf("failed to insert usage_request: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *Store) upsertDailyAggregateTx(ctx context.Context, tx *sql.Tx, dateStr string, req UsageRecord) error {
	now := time.Now().UTC()
	query := `
INSERT INTO daily_aggregates (
    date, key_id, model, provider, source, cost_kind,
    requests, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
    cost_micros, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8, $9, $10, $11, $12)
ON CONFLICT (date, key_id, model, provider, source, cost_kind) DO UPDATE SET
    requests = daily_aggregates.requests + 1,
    input_tokens = daily_aggregates.input_tokens + EXCLUDED.input_tokens,
    output_tokens = daily_aggregates.output_tokens + EXCLUDED.output_tokens,
    cache_read_tokens = daily_aggregates.cache_read_tokens + EXCLUDED.cache_read_tokens,
    cache_write_tokens = daily_aggregates.cache_write_tokens + EXCLUDED.cache_write_tokens,
    cost_micros = daily_aggregates.cost_micros + EXCLUDED.cost_micros,
    updated_at = EXCLUDED.updated_at`

	_, err := tx.ExecContext(ctx, query,
		dateStr, req.KeyID, req.ModelUsed, req.Provider, req.Source, req.CostKind,
		req.InputTokens, req.OutputTokens, req.CacheReadTokens, req.CacheWriteTokens,
		req.CostMicros, now,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert daily_aggregate: %w", err)
	}
	return nil
}

func (s *Store) RecordAudit(ctx context.Context, audit AuditEvent) error {
	unlock := s.lockSQLite()
	defer unlock()

	query := `
INSERT INTO events_audit (id, event_type, subject, payload_json, created_at)
VALUES ($1, $2, $3, $4, $5)`

	_, err := s.db.ExecContext(ctx, query,
		audit.ID, audit.EventType, audit.Subject, audit.PayloadJSON, audit.CreatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("failed to record audit: %w", err)
	}
	return nil
}

func (s *Store) UpsertCursor(ctx context.Context, cursor ImportCursor) error {
	unlock := s.lockSQLite()
	defer unlock()

	query := `
INSERT INTO import_cursors (source, file_path, byte_offset, last_line, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (source, file_path) DO UPDATE SET
    byte_offset = EXCLUDED.byte_offset,
    last_line = EXCLUDED.last_line,
    updated_at = EXCLUDED.updated_at`

	_, err := s.db.ExecContext(ctx, query,
		cursor.Source, cursor.FilePath, cursor.ByteOffset, cursor.LastLine, cursor.UpdatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("failed to upsert import cursor: %w", err)
	}
	return nil
}

func (s *Store) GetCursor(ctx context.Context, source, filePath string) (*ImportCursor, error) {
	query := `
SELECT source, file_path, byte_offset, last_line, updated_at
FROM import_cursors
WHERE source = $1 AND file_path = $2`

	var c ImportCursor
	err := s.db.QueryRowContext(ctx, query, source, filePath).Scan(
		&c.Source, &c.FilePath, &c.ByteOffset, &c.LastLine, &c.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get import cursor: %w", err)
	}
	return &c, nil
}

func (s *Store) CreateImportRun(ctx context.Context, run ImportRun) error {
	unlock := s.lockSQLite()
	defer unlock()

	query := `
INSERT INTO import_runs (
    id, source, status, lines_read, records_imported, errors_count, error_details, started_at, finished_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	var fin *time.Time
	if run.FinishedAt != nil {
		t := run.FinishedAt.UTC()
		fin = &t
	}

	_, err := s.db.ExecContext(ctx, query,
		run.ID, run.Source, run.Status, run.LinesRead, run.RecordsImported,
		run.ErrorsCount, run.ErrorDetails, run.StartedAt.UTC(), fin,
	)
	if err != nil {
		return fmt.Errorf("failed to create import run: %w", err)
	}
	return nil
}

func (s *Store) UpdateImportRun(ctx context.Context, run ImportRun) error {
	unlock := s.lockSQLite()
	defer unlock()

	query := `
UPDATE import_runs SET
    status = $2,
    lines_read = $3,
    records_imported = $4,
    errors_count = $5,
    error_details = $6,
    finished_at = $7
WHERE id = $1`

	var fin *time.Time
	if run.FinishedAt != nil {
		t := run.FinishedAt.UTC()
		fin = &t
	}

	res, err := s.db.ExecContext(ctx, query,
		run.ID, run.Status, run.LinesRead, run.RecordsImported,
		run.ErrorsCount, run.ErrorDetails, fin,
	)
	if err != nil {
		return fmt.Errorf("failed to update import run: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ListImportRuns(ctx context.Context, source string, limit int) ([]ImportRun, error) {
	if limit <= 0 {
		limit = 50
	}

	var (
		rows *sql.Rows
		err  error
	)
	if source != "" {
		query := `
SELECT id, source, status, lines_read, records_imported, errors_count, error_details, started_at, finished_at
FROM import_runs
WHERE source = $1
ORDER BY started_at DESC
LIMIT $2`
		rows, err = s.db.QueryContext(ctx, query, source, limit)
	} else {
		query := `
SELECT id, source, status, lines_read, records_imported, errors_count, error_details, started_at, finished_at
FROM import_runs
ORDER BY started_at DESC
LIMIT $1`
		rows, err = s.db.QueryContext(ctx, query, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list import runs: %w", err)
	}
	defer rows.Close()

	var results []ImportRun
	for rows.Next() {
		var r ImportRun
		var fin sql.NullTime
		if err := rows.Scan(
			&r.ID, &r.Source, &r.Status, &r.LinesRead, &r.RecordsImported,
			&r.ErrorsCount, &r.ErrorDetails, &r.StartedAt, &fin,
		); err != nil {
			return nil, err
		}
		if fin.Valid {
			r.FinishedAt = &fin.Time
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Store) CreateSubscription(ctx context.Context, sub Subscription) error {
	unlock := s.lockSQLite()
	defer unlock()

	query := `
INSERT INTO subscriptions (id, name, provider, monthly_cost_micros, renewal_day, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    provider = EXCLUDED.provider,
    monthly_cost_micros = EXCLUDED.monthly_cost_micros,
    renewal_day = EXCLUDED.renewal_day,
    updated_at = EXCLUDED.updated_at`

	_, err := s.db.ExecContext(ctx, query,
		sub.ID, sub.Name, sub.Provider, sub.MonthlyCostMicros, sub.RenewalDay,
		sub.CreatedAt.UTC(), sub.UpdatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("failed to create subscription: %w", err)
	}
	return nil
}

func (s *Store) GetSubscription(ctx context.Context, id string) (*Subscription, error) {
	query := `
SELECT id, name, provider, monthly_cost_micros, renewal_day, created_at, updated_at
FROM subscriptions
WHERE id = $1`

	var sub Subscription
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&sub.ID, &sub.Name, &sub.Provider, &sub.MonthlyCostMicros,
		&sub.RenewalDay, &sub.CreatedAt, &sub.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get subscription: %w", err)
	}
	return &sub, nil
}

func (s *Store) ListSubscriptions(ctx context.Context) ([]Subscription, error) {
	query := `
SELECT id, name, provider, monthly_cost_micros, renewal_day, created_at, updated_at
FROM subscriptions
ORDER BY name ASC`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list subscriptions: %w", err)
	}
	defer rows.Close()

	var results []Subscription
	for rows.Next() {
		var sub Subscription
		if err := rows.Scan(
			&sub.ID, &sub.Name, &sub.Provider, &sub.MonthlyCostMicros,
			&sub.RenewalDay, &sub.CreatedAt, &sub.UpdatedAt,
		); err != nil {
			return nil, err
		}
		results = append(results, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Store) DeleteSubscription(ctx context.Context, id string) error {
	unlock := s.lockSQLite()
	defer unlock()

	res, err := s.db.ExecContext(ctx, "DELETE FROM subscriptions WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to delete subscription: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func parseTimeFilter(val string, endOfDay bool) any {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, val); err == nil {
			t = t.UTC()
			if l == "2006-01-02" && endOfDay {
				t = time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 999999999, time.UTC)
			}
			return t
		}
	}
	return val
}

func buildUsageWhereClause(filter UsageFilter) (string, []any) {
	var conds []string
	var args []any
	idx := 1

	addCond := func(col, val string) {
		if val != "" {
			conds = append(conds, fmt.Sprintf("%s = $%d", col, idx))
			args = append(args, val)
			idx++
		}
	}

	if filter.StartDate != "" {
		conds = append(conds, fmt.Sprintf("created_at >= $%d", idx))
		args = append(args, parseTimeFilter(filter.StartDate, false))
		idx++
	}
	if filter.EndDate != "" {
		conds = append(conds, fmt.Sprintf("created_at <= $%d", idx))
		args = append(args, parseTimeFilter(filter.EndDate, true))
		idx++
	}

	addCond("key_id", filter.KeyID)
	addCond("model_used", filter.Model)
	addCond("provider", filter.Provider)
	addCond("source", filter.Source)
	addCond("cost_kind", filter.CostKind)

	if len(conds) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

func (s *Store) QueryUsage(ctx context.Context, filter UsageFilter) ([]UsageRecord, int, error) {
	whereClause, args := buildUsageWhereClause(filter)

	countQuery := "SELECT COUNT(*) FROM usage_requests" + whereClause
	var total int
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("failed to count usage: %w", err)
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	selectArgs := make([]any, len(args), len(args)+2)
	copy(selectArgs, args)
	paramIdx := len(args) + 1
	limitClause := fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", paramIdx, paramIdx+1)
	selectArgs = append(selectArgs, limit, offset)

	query := `
SELECT request_id, key_id, project, model_requested, model_used, provider, source, cost_kind,
       input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
       cost_micros, latency_ms, status, cache_hit, fallback_from, created_at
FROM usage_requests` + whereClause + limitClause

	rows, err := s.db.QueryContext(ctx, query, selectArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query usage: %w", err)
	}
	defer rows.Close()

	var records []UsageRecord
	for rows.Next() {
		var r UsageRecord
		if err := rows.Scan(
			&r.RequestID, &r.KeyID, &r.Project, &r.ModelRequested, &r.ModelUsed,
			&r.Provider, &r.Source, &r.CostKind, &r.InputTokens, &r.OutputTokens,
			&r.CacheReadTokens, &r.CacheWriteTokens, &r.CostMicros, &r.LatencyMs,
			&r.Status, &r.CacheHit, &r.FallbackFrom, &r.CreatedAt,
		); err != nil {
			return nil, 0, err
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return records, total, nil
}

func (s *Store) QueryDailyAggregates(ctx context.Context, startDate, endDate string) ([]DailyAggregate, error) {
	var (
		whereClause string
		args        []any
	)
	switch {
	case startDate != "" && endDate != "":
		whereClause = "WHERE date >= $1 AND date <= $2"
		args = append(args, startDate, endDate)
	case startDate != "":
		whereClause = "WHERE date >= $1"
		args = append(args, startDate)
	case endDate != "":
		whereClause = "WHERE date <= $1"
		args = append(args, endDate)
	}

	query := `
SELECT date, key_id, model, provider, source, cost_kind,
       requests, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
       cost_micros, updated_at
FROM daily_aggregates ` + whereClause + `
ORDER BY date ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query daily aggregates: %w", err)
	}
	defer rows.Close()

	var results []DailyAggregate
	for rows.Next() {
		var a DailyAggregate
		if err := rows.Scan(
			&a.Date, &a.KeyID, &a.Model, &a.Provider, &a.Source, &a.CostKind,
			&a.Requests, &a.InputTokens, &a.OutputTokens, &a.CacheReadTokens, &a.CacheWriteTokens,
			&a.CostMicros, &a.UpdatedAt,
		); err != nil {
			return nil, err
		}
		results = append(results, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}
