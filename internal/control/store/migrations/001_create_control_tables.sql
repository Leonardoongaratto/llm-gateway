CREATE TABLE IF NOT EXISTS keys (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    allowed_models TEXT NOT NULL DEFAULT '[]',
    daily_limit_micros BIGINT NOT NULL DEFAULT 0,
    monthly_limit_micros BIGINT NOT NULL DEFAULT 0,
    rpm INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP,
    revoked_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_keys_hash ON keys(key_hash);

CREATE TABLE IF NOT EXISTS pricing (
    model TEXT NOT NULL,
    provider TEXT NOT NULL,
    input_per_mtok_micros BIGINT NOT NULL DEFAULT 0,
    output_per_mtok_micros BIGINT NOT NULL DEFAULT 0,
    cache_read_per_mtok_micros BIGINT NOT NULL DEFAULT 0,
    cache_write_per_mtok_micros BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (model, provider)
);

CREATE TABLE IF NOT EXISTS logical_models (
    name TEXT PRIMARY KEY,
    chain TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS config_versions (
    id INTEGER PRIMARY KEY,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMP NOT NULL
);

INSERT INTO config_versions (id, version, updated_at)
VALUES (1, 1, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS control_outbox (
    id TEXT PRIMARY KEY,
    subject TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    published_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_control_outbox_pending ON control_outbox(published_at);
