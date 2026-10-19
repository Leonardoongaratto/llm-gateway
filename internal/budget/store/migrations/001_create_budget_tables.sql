CREATE TABLE IF NOT EXISTS key_limits (
    key_id TEXT PRIMARY KEY,
    daily_limit_micros BIGINT NOT NULL DEFAULT 0,
    monthly_limit_micros BIGINT NOT NULL DEFAULT 0,
    rpm INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS budget_periods (
    key_id TEXT NOT NULL,
    period TEXT NOT NULL,
    period_kind TEXT NOT NULL,
    spent_micros BIGINT NOT NULL DEFAULT 0,
    reserved_micros BIGINT NOT NULL DEFAULT 0,
    limit_micros BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (key_id, period)
);

CREATE TABLE IF NOT EXISTS reservations (
    id TEXT PRIMARY KEY,
    key_id TEXT NOT NULL,
    day_period TEXT NOT NULL,
    month_period TEXT NOT NULL,
    max_micros BIGINT NOT NULL,
    actual_micros BIGINT NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reservations_sweeper ON reservations(state, expires_at);

CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY,
    subject TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    published_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(published_at);
