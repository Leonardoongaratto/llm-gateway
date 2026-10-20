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

	eventsv1 "github.com/Leonardoongaratto/llm-gateway/gen/lgw/events/v1"
	"github.com/Leonardoongaratto/llm-gateway/internal/budget/core"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/ids"
	"github.com/Leonardoongaratto/llm-gateway/internal/kernel/money"
	"github.com/Leonardoongaratto/llm-gateway/internal/platform/sqlx"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate runs migrations for the budget store.
func Migrate(ctx context.Context, db *sql.DB) error {
	return sqlx.Migrate(ctx, db, migrationsFS, "migrations")
}

type Store struct {
	db          *sql.DB
	isSQLite    bool
	mu          sync.Mutex
	rateLimiter *core.SlidingWindowRateLimiter
}

func New(db *sql.DB) *Store {
	driverName := fmt.Sprintf("%T", db.Driver())
	isSQLite := strings.Contains(strings.ToLower(driverName), "sqlite")
	return &Store{
		db:          db,
		isSQLite:    isSQLite,
		rateLimiter: core.NewSlidingWindowRateLimiter(),
	}
}

func (s *Store) DB() *sql.DB {
	return s.db
}

type ReserveParams struct {
	KeyID     string
	MaxMicros int64
	RequestID string
	Now       time.Time
	TTL       time.Duration
}

type ReserveResult struct {
	ReservationID          string
	Allowed                bool
	RemainingDailyMicros   int64
	RemainingMonthlyMicros int64
}

type SettleResult struct {
	Settled                bool
	RemainingDailyMicros   int64
	RemainingMonthlyMicros int64
}

type RemainingInfo struct {
	KeyID                 string
	DailyLimitMicros      int64
	MonthlyLimitMicros    int64
	DailySpentMicros      int64
	DailyReservedMicros   int64
	MonthlySpentMicros    int64
	MonthlyReservedMicros int64
}

type OutboxMessage struct {
	ID        string
	Subject   string
	Payload   string
	CreatedAt time.Time
}

func (s *Store) SetKeyLimits(ctx context.Context, limits core.KeyLimits) error {
	if s.isSQLite {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	query := `
INSERT INTO key_limits (key_id, daily_limit_micros, monthly_limit_micros, rpm, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (key_id) DO UPDATE SET
    daily_limit_micros = EXCLUDED.daily_limit_micros,
    monthly_limit_micros = EXCLUDED.monthly_limit_micros,
    rpm = EXCLUDED.rpm,
    updated_at = EXCLUDED.updated_at;
`
	_, err := s.db.ExecContext(ctx, query,
		limits.KeyID,
		limits.DailyLimitMicros,
		limits.MonthlyLimitMicros,
		limits.RPM,
		time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("failed to set key limits: %w", err)
	}
	return nil
}

// DeleteKeyLimits removes limits for a key, causing any subsequent reservation to be denied.
func (s *Store) DeleteKeyLimits(ctx context.Context, keyID string) error {
	if s.isSQLite {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	query := `DELETE FROM key_limits WHERE key_id = $1`
	_, err := s.db.ExecContext(ctx, query, keyID)
	return err
}

// GetKeyLimits retrieves the current limits configuration for the specified keyID.
func (s *Store) GetKeyLimits(ctx context.Context, keyID string) (*core.KeyLimits, error) {
	if s.isSQLite {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	return s.queryKeyLimits(ctx, tx, keyID, false)
}

func (s *Store) queryKeyLimits(ctx context.Context, tx *sql.Tx, keyID string, forUpdate bool) (*core.KeyLimits, error) {
	query := `SELECT daily_limit_micros, monthly_limit_micros, rpm FROM key_limits WHERE key_id = $1`
	if !s.isSQLite && forUpdate {
		query += " FOR UPDATE"
	}

	row := tx.QueryRowContext(ctx, query, keyID)
	var daily, monthly int64
	var rpm int32
	if err := row.Scan(&daily, &monthly, &rpm); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrNotFound
		}
		return nil, err
	}
	return &core.KeyLimits{
		KeyID:              keyID,
		DailyLimitMicros:   money.Micros(daily),
		MonthlyLimitMicros: money.Micros(monthly),
		RPM:                rpm,
	}, nil
}

func (s *Store) queryPeriodBalances(ctx context.Context, tx *sql.Tx, keyID, period string, forUpdate bool) (spent, reserved int64, err error) {
	query := `SELECT spent_micros, reserved_micros FROM budget_periods WHERE key_id = $1 AND period = $2`
	if !s.isSQLite && forUpdate {
		query += " FOR UPDATE"
	}
	row := tx.QueryRowContext(ctx, query, keyID, period)
	if err := row.Scan(&spent, &reserved); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	return spent, reserved, nil
}

func (s *Store) upsertReservedPeriod(ctx context.Context, tx *sql.Tx, keyID, period, periodKind string, amount, limit int64) error {
	query := `
INSERT INTO budget_periods (key_id, period, period_kind, spent_micros, reserved_micros, limit_micros)
VALUES ($1, $2, $3, 0, $4, $5)
ON CONFLICT (key_id, period) DO UPDATE SET
    reserved_micros = budget_periods.reserved_micros + EXCLUDED.reserved_micros;
`
	_, err := tx.ExecContext(ctx, query, keyID, period, periodKind, amount, limit)
	return err
}

func insertReservationRecord(ctx context.Context, tx *sql.Tx, keyID, dayPeriod, monthPeriod string, maxMicros int64, expiresAt, now time.Time) (string, error) {
	resID, err := ids.NewReservationID()
	if err != nil {
		return "", err
	}

	insertRes := `
INSERT INTO reservations (id, key_id, day_period, month_period, max_micros, actual_micros, state, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, 0, 'reserved', $6, $7, $8);
`
	_, err = tx.ExecContext(ctx, insertRes, string(resID), keyID, dayPeriod, monthPeriod, maxMicros, expiresAt, now, now)
	return string(resID), err
}

func (s *Store) checkBalancesAndReserve(ctx context.Context, tx *sql.Tx, limits core.KeyLimits, keyID, dayPeriod, monthPeriod string, maxMicros int64) (remDaily, remMonthly int64, err error) {
	daySpent, dayReserved, err := s.queryPeriodBalances(ctx, tx, keyID, dayPeriod, true)
	if err != nil {
		return 0, 0, err
	}
	monthSpent, monthReserved, err := s.queryPeriodBalances(ctx, tx, keyID, monthPeriod, true)
	if err != nil {
		return 0, 0, err
	}

	if err := core.CheckLimits(limits, money.Micros(daySpent), money.Micros(dayReserved), money.Micros(monthSpent), money.Micros(monthReserved), money.Micros(maxMicros)); err != nil {
		return 0, 0, err
	}

	if err := s.upsertReservedPeriod(ctx, tx, keyID, dayPeriod, "day", maxMicros, int64(limits.DailyLimitMicros)); err != nil {
		return 0, 0, err
	}
	if err := s.upsertReservedPeriod(ctx, tx, keyID, monthPeriod, "month", maxMicros, int64(limits.MonthlyLimitMicros)); err != nil {
		return 0, 0, err
	}

	if limits.DailyLimitMicros <= 0 {
		remDaily = -1
	} else {
		remDaily = int64(limits.DailyLimitMicros) - (daySpent + dayReserved + maxMicros)
		if remDaily < 0 {
			remDaily = 0
		}
	}

	if limits.MonthlyLimitMicros <= 0 {
		remMonthly = -1
	} else {
		remMonthly = int64(limits.MonthlyLimitMicros) - (monthSpent + monthReserved + maxMicros)
		if remMonthly < 0 {
			remMonthly = 0
		}
	}
	return remDaily, remMonthly, nil
}

func (s *Store) Reserve(ctx context.Context, params ReserveParams) (*ReserveResult, error) {
	if s.isSQLite {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	if params.Now.IsZero() {
		params.Now = time.Now()
	}
	params.Now = params.Now.UTC()
	if params.TTL <= 0 {
		params.TTL = 15 * time.Minute
	}

	dayPeriod := core.DayPeriod(params.Now)
	monthPeriod := core.MonthPeriod(params.Now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	limits, err := s.queryKeyLimits(ctx, tx, params.KeyID, true)
	if err != nil {
		return nil, err
	}

	if limits.RPM > 0 {
		if allowed, _ := s.rateLimiter.Allow(params.KeyID, limits.RPM, params.Now); !allowed {
			return nil, core.ErrRateLimited
		}
	}

	remDaily, remMonthly, err := s.checkBalancesAndReserve(ctx, tx, *limits, params.KeyID, dayPeriod, monthPeriod, params.MaxMicros)
	if err != nil {
		return nil, err
	}

	resID, err := insertReservationRecord(ctx, tx, params.KeyID, dayPeriod, monthPeriod, params.MaxMicros, params.Now.Add(params.TTL), params.Now)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &ReserveResult{
		ReservationID:          resID,
		Allowed:                true,
		RemainingDailyMicros:   remDaily,
		RemainingMonthlyMicros: remMonthly,
	}, nil
}

type reservationRecord struct {
	KeyID       string
	DayPeriod   string
	MonthPeriod string
	MaxMicros   int64
	State       string
}

func (s *Store) queryReservation(ctx context.Context, tx *sql.Tx, reservationID string) (*reservationRecord, error) {
	query := `SELECT key_id, day_period, month_period, max_micros, state FROM reservations WHERE id = $1`
	if !s.isSQLite {
		query += " FOR UPDATE"
	}
	row := tx.QueryRowContext(ctx, query, reservationID)
	var rec reservationRecord
	if err := row.Scan(&rec.KeyID, &rec.DayPeriod, &rec.MonthPeriod, &rec.MaxMicros, &rec.State); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrNotFound
		}
		return nil, err
	}
	return &rec, nil
}

func (s *Store) settlePeriod(ctx context.Context, tx *sql.Tx, keyID, period string, maxMicros, actualMicros, currentLimit int64) (spentBefore int64, err error) {
	query := `SELECT spent_micros FROM budget_periods WHERE key_id = $1 AND period = $2`
	if !s.isSQLite {
		query += " FOR UPDATE"
	}
	row := tx.QueryRowContext(ctx, query, keyID, period)
	if err := row.Scan(&spentBefore); err != nil {
		return 0, err
	}

	updateQuery := `
UPDATE budget_periods
SET reserved_micros = reserved_micros - $1,
    spent_micros = spent_micros + $2,
    limit_micros = $3
WHERE key_id = $4 AND period = $5;
`
	_, err = tx.ExecContext(ctx, updateQuery, maxMicros, actualMicros, currentLimit, keyID, period)
	return spentBefore, err
}

func (s *Store) checkThreshold(ctx context.Context, tx *sql.Tx, keyID, period string, limit, spentBefore, actual int64, now time.Time) error {
	if limit <= 0 {
		return nil
	}
	newSpent := spentBefore + actual
	oldPct := (spentBefore * 100) / limit
	newPct := (newSpent * 100) / limit

	var levels []int32
	if oldPct < 80 && newPct >= 80 {
		levels = append(levels, 80)
	}
	if oldPct < 100 && newPct >= 100 {
		levels = append(levels, 100)
	}

	for _, lvl := range levels {
		evt := &eventsv1.BudgetThreshold{
			KeyId:       keyID,
			Period:      period,
			Level:       lvl,
			SpentMicros: newSpent,
			LimitMicros: limit,
			OccurredAt:  timestamppb.New(now),
		}
		payloadBytes, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(evt)
		if err != nil {
			return err
		}
		msgID, err := ids.NewRequestID()
		if err != nil {
			return err
		}
		insertOutbox := `INSERT INTO outbox (id, subject, payload, created_at) VALUES ($1, $2, $3, $4);`
		if _, err := tx.ExecContext(ctx, insertOutbox, string(msgID), "lgw.budget.threshold.v1", string(payloadBytes), now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Settle(ctx context.Context, reservationID string, actualMicros int64, now time.Time) (*SettleResult, error) {
	if s.isSQLite {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	rec, err := s.queryReservation(ctx, tx, reservationID)
	if err != nil {
		return nil, err
	}

	if rec.State == "settled" {
		return &SettleResult{Settled: true}, nil
	}
	if rec.State != "reserved" {
		return nil, core.ErrReservationClosed
	}

	limits, err := s.queryKeyLimits(ctx, tx, rec.KeyID, true)
	if err != nil {
		return nil, err
	}

	updateRes := `UPDATE reservations SET state = 'settled', actual_micros = $1, updated_at = $2 WHERE id = $3;`
	if _, err := tx.ExecContext(ctx, updateRes, actualMicros, now, reservationID); err != nil {
		return nil, err
	}

	daySpentBefore, err := s.settlePeriod(ctx, tx, rec.KeyID, rec.DayPeriod, rec.MaxMicros, actualMicros, int64(limits.DailyLimitMicros))
	if err != nil {
		return nil, err
	}
	monthSpentBefore, err := s.settlePeriod(ctx, tx, rec.KeyID, rec.MonthPeriod, rec.MaxMicros, actualMicros, int64(limits.MonthlyLimitMicros))
	if err != nil {
		return nil, err
	}

	if err := s.checkThreshold(ctx, tx, rec.KeyID, "day", int64(limits.DailyLimitMicros), daySpentBefore, actualMicros, now); err != nil {
		return nil, err
	}
	if err := s.checkThreshold(ctx, tx, rec.KeyID, "month", int64(limits.MonthlyLimitMicros), monthSpentBefore, actualMicros, now); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &SettleResult{Settled: true}, nil
}

func (s *Store) Release(ctx context.Context, reservationID string, now time.Time) error {
	if s.isSQLite {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	rec, err := s.queryReservation(ctx, tx, reservationID)
	if err != nil {
		return err
	}

	if rec.State == "released" {
		return nil
	}
	if rec.State != "reserved" {
		return core.ErrReservationClosed
	}

	updateRes := `UPDATE reservations SET state = 'released', updated_at = $1 WHERE id = $2;`
	if _, err := tx.ExecContext(ctx, updateRes, now, reservationID); err != nil {
		return err
	}

	refundQuery := `UPDATE budget_periods SET reserved_micros = reserved_micros - $1 WHERE key_id = $2 AND period = $3;`
	if _, err := tx.ExecContext(ctx, refundQuery, rec.MaxMicros, rec.KeyID, rec.DayPeriod); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, refundQuery, rec.MaxMicros, rec.KeyID, rec.MonthPeriod); err != nil {
		return err
	}

	return tx.Commit()
}

type expItem struct {
	id, keyID, day, month string
	max                   int64
}

func fetchExpiredReservations(ctx context.Context, tx *sql.Tx, now time.Time, isSQLite bool) ([]expItem, error) {
	query := `SELECT id, key_id, day_period, month_period, max_micros FROM reservations WHERE state = 'reserved' AND expires_at < $1`
	if !isSQLite {
		query += " FOR UPDATE"
	}

	rows, err := tx.QueryContext(ctx, query, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var expired []expItem
	for rows.Next() {
		var item expItem
		if err := rows.Scan(&item.id, &item.keyID, &item.day, &item.month, &item.max); err != nil {
			return nil, err
		}
		expired = append(expired, item)
	}
	return expired, rows.Err()
}

func expireSingleReservation(ctx context.Context, tx *sql.Tx, item expItem, now time.Time) error {
	updateRes := `UPDATE reservations SET state = 'expired', updated_at = $1 WHERE id = $2;`
	if _, err := tx.ExecContext(ctx, updateRes, now, item.id); err != nil {
		return err
	}
	refundQuery := `UPDATE budget_periods SET reserved_micros = reserved_micros - $1 WHERE key_id = $2 AND period = $3;`
	if _, err := tx.ExecContext(ctx, refundQuery, item.max, item.keyID, item.day); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, refundQuery, item.max, item.keyID, item.month)
	return err
}

func (s *Store) ExpireReservations(ctx context.Context, now time.Time) (int, error) {
	if s.isSQLite {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	expired, err := fetchExpiredReservations(ctx, tx, now, s.isSQLite)
	if err != nil {
		return 0, err
	}

	for _, item := range expired {
		if err := expireSingleReservation(ctx, tx, item, now); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(expired), nil
}

func (s *Store) Remaining(ctx context.Context, keyID string, now time.Time) (*RemainingInfo, error) {
	if s.isSQLite {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()

	dayPeriod := core.DayPeriod(now)
	monthPeriod := core.MonthPeriod(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	limits, err := s.queryKeyLimits(ctx, tx, keyID, false)
	if err != nil {
		return nil, err
	}

	daySpent, dayReserved, err := s.queryPeriodBalances(ctx, tx, keyID, dayPeriod, false)
	if err != nil {
		return nil, err
	}
	monthSpent, monthReserved, err := s.queryPeriodBalances(ctx, tx, keyID, monthPeriod, false)
	if err != nil {
		return nil, err
	}

	return &RemainingInfo{
		KeyID:                 keyID,
		DailyLimitMicros:      int64(limits.DailyLimitMicros),
		MonthlyLimitMicros:    int64(limits.MonthlyLimitMicros),
		DailySpentMicros:      daySpent,
		DailyReservedMicros:   dayReserved,
		MonthlySpentMicros:    monthSpent,
		MonthlyReservedMicros: monthReserved,
	}, nil
}

func (s *Store) FetchPendingOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	if s.isSQLite {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	if limit <= 0 {
		limit = 50
	}

	query := `SELECT id, subject, payload, created_at FROM outbox WHERE published_at IS NULL ORDER BY created_at ASC, id ASC LIMIT $1;`
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

func (s *Store) MarkOutboxPublished(ctx context.Context, ids []string, publishedAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}

	if s.isSQLite {
		s.mu.Lock()
		defer s.mu.Unlock()
	}

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids)+1)
	args[0] = publishedAt
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = id
	}

	query := fmt.Sprintf("UPDATE outbox SET published_at = $1 WHERE id IN (%s);", strings.Join(placeholders, ", "))
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}
