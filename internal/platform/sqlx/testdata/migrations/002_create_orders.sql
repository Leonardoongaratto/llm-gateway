CREATE TABLE IF NOT EXISTS test_orders (
    id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL,
    amount_micros BIGINT NOT NULL
);
