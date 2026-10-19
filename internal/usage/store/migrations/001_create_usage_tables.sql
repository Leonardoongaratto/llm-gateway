CREATE TABLE IF NOT EXISTS usage_requests (
    request_id TEXT PRIMARY KEY,
    key_id TEXT NOT NULL DEFAULT '',
    project TEXT NOT NULL DEFAULT '',
    model_requested TEXT NOT NULL DEFAULT '',
    model_used TEXT NOT NULL,
    provider TEXT NOT NULL,
    source TEXT NOT NULL,
    cost_kind TEXT NOT NULL,
    input_tokens BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    cache_read_tokens BIGINT NOT NULL DEFAULT 0,
    cache_write_tokens BIGINT NOT NULL DEFAULT 0,
    cost_micros BIGINT NOT NULL DEFAULT 0,
    latency_ms BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'success',
    cache_hit BOOLEAN NOT NULL DEFAULT FALSE,
    fallback_from TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_usage_requests_created_at ON usage_requests(created_at);
CREATE INDEX IF NOT EXISTS idx_usage_requests_key_id ON usage_requests(key_id);
CREATE INDEX IF NOT EXISTS idx_usage_requests_source ON usage_requests(source);
CREATE INDEX IF NOT EXISTS idx_usage_requests_model ON usage_requests(model_used);
CREATE INDEX IF NOT EXISTS idx_usage_requests_provider ON usage_requests(provider);

CREATE TABLE IF NOT EXISTS daily_aggregates (
    date TEXT NOT NULL,
    key_id TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL,
    provider TEXT NOT NULL,
    source TEXT NOT NULL,
    cost_kind TEXT NOT NULL,
    requests BIGINT NOT NULL DEFAULT 0,
    input_tokens BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    cache_read_tokens BIGINT NOT NULL DEFAULT 0,
    cache_write_tokens BIGINT NOT NULL DEFAULT 0,
    cost_micros BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (date, key_id, model, provider, source, cost_kind)
);

CREATE INDEX IF NOT EXISTS idx_daily_aggregates_date ON daily_aggregates(date);
CREATE INDEX IF NOT EXISTS idx_daily_aggregates_key_id ON daily_aggregates(key_id);

CREATE TABLE IF NOT EXISTS events_audit (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    subject TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_audit_type_created ON events_audit(event_type, created_at);

CREATE TABLE IF NOT EXISTS processed_messages (
    message_id TEXT PRIMARY KEY,
    stream TEXT NOT NULL,
    consumer TEXT NOT NULL,
    processed_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_processed_messages_stream ON processed_messages(stream);

CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    provider TEXT NOT NULL,
    monthly_cost_micros BIGINT NOT NULL DEFAULT 0,
    renewal_day INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS import_cursors (
    source TEXT NOT NULL,
    file_path TEXT NOT NULL,
    byte_offset BIGINT NOT NULL DEFAULT 0,
    last_line BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (source, file_path)
);

CREATE TABLE IF NOT EXISTS import_runs (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    lines_read BIGINT NOT NULL DEFAULT 0,
    records_imported BIGINT NOT NULL DEFAULT 0,
    errors_count BIGINT NOT NULL DEFAULT 0,
    error_details TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_import_runs_source ON import_runs(source, started_at);
