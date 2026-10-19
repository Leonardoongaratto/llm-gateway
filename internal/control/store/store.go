package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/control/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate executes control schema migrations.
func Migrate(ctx context.Context, db *sql.DB) error {
	return sqlx.Migrate(ctx, db, migrationsFS, "migrations")
}

// OutboxMessage holds an unpublished control domain event.
type OutboxMessage struct {
	ID        string
	Subject   string
	Payload   string
	CreatedAt time.Time
}

// Store handles persistence for keys, pricing, logical models, and config versioning.
type Store struct {
	db       *sql.DB
	isSQLite bool
	mu       sync.Mutex
}

// New creates a new Store instance.
func New(db *sql.DB) *Store {
	driverName := fmt.Sprintf("%T", db.Driver())
	isSQLite := strings.Contains(strings.ToLower(driverName), "sqlite")
	return &Store{
		db:       db,
		isSQLite: isSQLite,
	}
}

// DB returns the underlying sql.DB instance.
func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) lockForSQLite() {
	if s.isSQLite {
		s.mu.Lock()
	}
}

func (s *Store) unlockForSQLite() {
	if s.isSQLite {
		s.mu.Unlock()
	}
}

func serializeStringSlice(items []string) string {
	if len(items) == 0 {
		return "[]"
	}
	b, err := json.Marshal(items)
	if err != nil {
		return "[]"
	}
	return string(b)
}

func deserializeStringSlice(raw string) []string {
	if strings.TrimSpace(raw) == "" || raw == "[]" {
		return nil
	}
	var res []string
	if err := json.Unmarshal([]byte(raw), &res); err != nil {
		return nil
	}
	return res
}

func (s *Store) incrementVersionTx(ctx context.Context, tx *sql.Tx, now time.Time) (int64, error) {
	updateQuery := "UPDATE config_versions SET version = version + 1, updated_at = $1 WHERE id = 1"
	if _, err := tx.ExecContext(ctx, updateQuery, now); err != nil {
		return 0, fmt.Errorf("failed to increment config version: %w", err)
	}

	var version int64
	selectQuery := "SELECT version FROM config_versions WHERE id = 1"
	if err := tx.QueryRowContext(ctx, selectQuery).Scan(&version); err != nil {
		return 0, fmt.Errorf("failed to query incremented config version: %w", err)
	}
	return version, nil
}

func (s *Store) enqueueOutboxTx(ctx context.Context, tx *sql.Tx, subject string, msg proto.Message, now time.Time) error {
	opts := protojson.MarshalOptions{UseProtoNames: true}
	payloadBytes, err := opts.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox event: %w", err)
	}

	msgID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("failed to generate outbox message id: %w", err)
	}

	query := "INSERT INTO control_outbox (id, subject, payload, created_at) VALUES ($1, $2, $3, $4)"
	_, err = tx.ExecContext(ctx, query, msgID.String(), subject, string(payloadBytes), now)
	if err != nil {
		return fmt.Errorf("failed to insert outbox message: %w", err)
	}
	return nil
}

// GetCurrentVersion returns the latest configuration version.
func (s *Store) GetCurrentVersion(ctx context.Context) (int64, error) {
	var version int64
	query := "SELECT version FROM config_versions WHERE id = 1"
	if err := s.db.QueryRowContext(ctx, query).Scan(&version); err != nil {
		return 0, fmt.Errorf("failed to get current version: %w", err)
	}
	return version, nil
}

// CreateKey inserts a new virtual key, increments config version, and records an outbox event.
func (s *Store) CreateKey(ctx context.Context, key core.VirtualKey) (*core.VirtualKey, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}

	s.lockForSQLite()
	defer s.unlockForSQLite()

	if key.ID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("failed to generate key id: %w", err)
		}
		key.ID = id.String()
	}

	now := time.Now().UTC()
	if key.CreatedAt.IsZero() {
		key.CreatedAt = now
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	query := `
INSERT INTO keys (id, name, key_hash, allowed_models, daily_limit_micros, monthly_limit_micros, rpm, created_at, expires_at, revoked_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	_, err = tx.ExecContext(ctx, query,
		key.ID,
		key.Name,
		key.KeyHash,
		serializeStringSlice(key.AllowedModels),
		int64(key.DailyLimitMicros),
		int64(key.MonthlyLimitMicros),
		key.RPM,
		key.CreatedAt,
		key.ExpiresAt,
		key.RevokedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert virtual key: %w", err)
	}

	newVersion, err := s.incrementVersionTx(ctx, tx, now)
	if err != nil {
		return nil, err
	}

	cfgEvent := &eventsv1.ConfigChanged{
		Kind:    eventsv1.ConfigChanged_KEY,
		Version: newVersion,
		KeyLimits: []*eventsv1.KeyLimit{
			{
				KeyId:              key.ID,
				DailyLimitMicros:   int64(key.DailyLimitMicros),
				MonthlyLimitMicros: int64(key.MonthlyLimitMicros),
				Rpm:                key.RPM,
			},
		},
	}
	if err := s.enqueueOutboxTx(ctx, tx, "lgw.config.changed.v1", cfgEvent, now); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit create key tx: %w", err)
	}

	return &key, nil
}

func scanKey(row interface{ Scan(dest ...any) error }) (*core.VirtualKey, error) {
	var (
		k                    core.VirtualKey
		allowedRaw           string
		daily, monthly       int64
		expiresAt, revokedAt sql.NullTime
	)

	err := row.Scan(
		&k.ID,
		&k.Name,
		&k.KeyHash,
		&allowedRaw,
		&daily,
		&monthly,
		&k.RPM,
		&k.CreatedAt,
		&expiresAt,
		&revokedAt,
	)
	if err != nil {
		return nil, err
	}

	k.AllowedModels = deserializeStringSlice(allowedRaw)
	k.DailyLimitMicros = money.Micros(daily)
	k.MonthlyLimitMicros = money.Micros(monthly)
	if expiresAt.Valid {
		k.ExpiresAt = &expiresAt.Time
	}
	if revokedAt.Valid {
		k.RevokedAt = &revokedAt.Time
	}
	return &k, nil
}

// GetKeyByID retrieves a virtual key by its primary key identifier.
func (s *Store) GetKeyByID(ctx context.Context, id string) (*core.VirtualKey, error) {
	query := `
SELECT id, name, key_hash, allowed_models, daily_limit_micros, monthly_limit_micros, rpm, created_at, expires_at, revoked_at
FROM keys WHERE id = $1`
	row := s.db.QueryRowContext(ctx, query, id)
	k, err := scanKey(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrKeyNotFound
		}
		return nil, fmt.Errorf("failed to get key by id: %w", err)
	}
	return k, nil
}

// GetKeyByHash retrieves a virtual key by its SHA-256 peppered hash.
func (s *Store) GetKeyByHash(ctx context.Context, keyHash string) (*core.VirtualKey, error) {
	query := `
SELECT id, name, key_hash, allowed_models, daily_limit_micros, monthly_limit_micros, rpm, created_at, expires_at, revoked_at
FROM keys WHERE key_hash = $1`
	row := s.db.QueryRowContext(ctx, query, keyHash)
	k, err := scanKey(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrKeyNotFound
		}
		return nil, fmt.Errorf("failed to get key by hash: %w", err)
	}
	return k, nil
}

// ListKeys returns all virtual keys ordered by creation timestamp descending.
func (s *Store) ListKeys(ctx context.Context) ([]core.VirtualKey, error) {
	query := `
SELECT id, name, key_hash, allowed_models, daily_limit_micros, monthly_limit_micros, rpm, created_at, expires_at, revoked_at
FROM keys ORDER BY created_at DESC, id DESC`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list keys: %w", err)
	}
	defer rows.Close()

	var keys []core.VirtualKey
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		keys = append(keys, *k)
	}
	return keys, rows.Err()
}

// RevokeKey marks the key as revoked, increments config version, and posts an outbox event.
func (s *Store) RevokeKey(ctx context.Context, id string, revokedAt time.Time) error {
	s.lockForSQLite()
	defer s.unlockForSQLite()

	if revokedAt.IsZero() {
		revokedAt = time.Now().UTC()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	checkQuery := "SELECT revoked_at FROM keys WHERE id = $1"
	if !s.isSQLite {
		checkQuery += " FOR UPDATE"
	}

	var existingRevoked sql.NullTime
	if err := tx.QueryRowContext(ctx, checkQuery, id).Scan(&existingRevoked); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.ErrKeyNotFound
		}
		return fmt.Errorf("failed to check key before revoking: %w", err)
	}

	if existingRevoked.Valid {
		return core.ErrKeyRevoked
	}

	updateQuery := "UPDATE keys SET revoked_at = $1 WHERE id = $2"
	if _, err := tx.ExecContext(ctx, updateQuery, revokedAt, id); err != nil {
		return fmt.Errorf("failed to update key revoked_at: %w", err)
	}

	newVersion, err := s.incrementVersionTx(ctx, tx, revokedAt)
	if err != nil {
		return err
	}

	cfgEvent := &eventsv1.ConfigChanged{
		Kind:    eventsv1.ConfigChanged_KEY_REVOKED,
		Version: newVersion,
		KeyLimits: []*eventsv1.KeyLimit{
			{
				KeyId:   id,
				Revoked: true,
			},
		},
	}
	if err := s.enqueueOutboxTx(ctx, tx, "lgw.config.changed.v1", cfgEvent, revokedAt); err != nil {
		return err
	}

	return tx.Commit()
}

// ListPricing returns all active pricing configurations ordered by model and provider.
func (s *Store) ListPricing(ctx context.Context) ([]core.Pricing, error) {
	query := `
SELECT model, provider, input_per_mtok_micros, output_per_mtok_micros, cache_read_per_mtok_micros, cache_write_per_mtok_micros, updated_at
FROM pricing ORDER BY model ASC, provider ASC`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list pricing: %w", err)
	}
	defer rows.Close()

	var pricing []core.Pricing
	for rows.Next() {
		var (
			p                                    core.Pricing
			input, output, cacheRead, cacheWrite int64
		)
		err := rows.Scan(
			&p.Model,
			&p.Provider,
			&input,
			&output,
			&cacheRead,
			&cacheWrite,
			&p.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		p.InputPerMTok = money.Micros(input)
		p.OutputPerMTok = money.Micros(output)
		p.CacheReadPerMTok = money.Micros(cacheRead)
		p.CacheWritePerMTok = money.Micros(cacheWrite)
		pricing = append(pricing, p)
	}
	return pricing, rows.Err()
}

// UpsertPricing inserts or updates pricing records atomically and creates a config change event.
func (s *Store) UpsertPricing(ctx context.Context, items []core.Pricing) error {
	for i := range items {
		if err := items[i].Validate(); err != nil {
			return err
		}
	}

	s.lockForSQLite()
	defer s.unlockForSQLite()

	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	upsertQuery := `
INSERT INTO pricing (model, provider, input_per_mtok_micros, output_per_mtok_micros, cache_read_per_mtok_micros, cache_write_per_mtok_micros, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (model, provider) DO UPDATE SET
    input_per_mtok_micros = EXCLUDED.input_per_mtok_micros,
    output_per_mtok_micros = EXCLUDED.output_per_mtok_micros,
    cache_read_per_mtok_micros = EXCLUDED.cache_read_per_mtok_micros,
    cache_write_per_mtok_micros = EXCLUDED.cache_write_per_mtok_micros,
    updated_at = EXCLUDED.updated_at`

	for _, p := range items {
		_, err := tx.ExecContext(ctx, upsertQuery,
			p.Model,
			p.Provider,
			int64(p.InputPerMTok),
			int64(p.OutputPerMTok),
			int64(p.CacheReadPerMTok),
			int64(p.CacheWritePerMTok),
			now,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert pricing for %s/%s: %w", p.Model, p.Provider, err)
		}
	}

	newVersion, err := s.incrementVersionTx(ctx, tx, now)
	if err != nil {
		return err
	}

	cfgEvent := &eventsv1.ConfigChanged{
		Kind:    eventsv1.ConfigChanged_PRICING,
		Version: newVersion,
	}
	if err := s.enqueueOutboxTx(ctx, tx, "lgw.config.changed.v1", cfgEvent, now); err != nil {
		return err
	}

	return tx.Commit()
}

// ListLogicalModels returns all registered logical model alias mappings.
func (s *Store) ListLogicalModels(ctx context.Context) ([]core.LogicalModel, error) {
	query := "SELECT name, chain, updated_at FROM logical_models ORDER BY name ASC"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list logical models: %w", err)
	}
	defer rows.Close()

	var models []core.LogicalModel
	for rows.Next() {
		var (
			lm       core.LogicalModel
			chainRaw string
		)
		if err := rows.Scan(&lm.Name, &chainRaw, &lm.UpdatedAt); err != nil {
			return nil, err
		}
		lm.Chain = deserializeStringSlice(chainRaw)
		models = append(models, lm)
	}
	return models, rows.Err()
}

// UpsertLogicalModels inserts or updates logical model alias chains atomically.
func (s *Store) UpsertLogicalModels(ctx context.Context, models []core.LogicalModel) error {
	for i := range models {
		if err := models[i].Validate(); err != nil {
			return err
		}
	}

	s.lockForSQLite()
	defer s.unlockForSQLite()

	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	upsertQuery := `
INSERT INTO logical_models (name, chain, updated_at)
VALUES ($1, $2, $3)
ON CONFLICT (name) DO UPDATE SET
    chain = EXCLUDED.chain,
    updated_at = EXCLUDED.updated_at`

	for _, lm := range models {
		_, err := tx.ExecContext(ctx, upsertQuery, lm.Name, serializeStringSlice(lm.Chain), now)
		if err != nil {
			return fmt.Errorf("failed to upsert logical model %s: %w", lm.Name, err)
		}
	}

	newVersion, err := s.incrementVersionTx(ctx, tx, now)
	if err != nil {
		return err
	}

	cfgEvent := &eventsv1.ConfigChanged{
		Kind:    eventsv1.ConfigChanged_MODELS,
		Version: newVersion,
	}
	if err := s.enqueueOutboxTx(ctx, tx, "lgw.config.changed.v1", cfgEvent, now); err != nil {
		return err
	}

	return tx.Commit()
}

// GetSnapshot retrieves the full control configuration snapshot for distribution.
func (s *Store) GetSnapshot(ctx context.Context) (*core.Snapshot, error) {
	version, err := s.GetCurrentVersion(ctx)
	if err != nil {
		return nil, err
	}

	keys, err := s.ListKeys(ctx)
	if err != nil {
		return nil, err
	}

	pricing, err := s.ListPricing(ctx)
	if err != nil {
		return nil, err
	}

	logicalModels, err := s.ListLogicalModels(ctx)
	if err != nil {
		return nil, err
	}

	return &core.Snapshot{
		Version:       version,
		Keys:          keys,
		Pricing:       pricing,
		LogicalModels: logicalModels,
	}, nil
}

// FetchPendingOutbox retrieves unpublished outbox messages up to limit.
func (s *Store) FetchPendingOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	s.lockForSQLite()
	defer s.unlockForSQLite()

	if limit <= 0 {
		limit = 50
	}

	query := "SELECT id, subject, payload, created_at FROM control_outbox WHERE published_at IS NULL ORDER BY created_at ASC, id ASC LIMIT $1"
	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []OutboxMessage
	for rows.Next() {
		var m OutboxMessage
		if err := rows.Scan(&m.ID, &m.Subject, &m.Payload, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// MarkOutboxPublished updates published_at for the given outbox IDs.
func (s *Store) MarkOutboxPublished(ctx context.Context, ids []string, publishedAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}

	s.lockForSQLite()
	defer s.unlockForSQLite()

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids)+1)
	args[0] = publishedAt
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = id
	}

	query := fmt.Sprintf("UPDATE control_outbox SET published_at = $1 WHERE id IN (%s)", strings.Join(placeholders, ", "))
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

// Close closes the underlying database handle.
func (s *Store) Close() error {
	return s.db.Close()
}
