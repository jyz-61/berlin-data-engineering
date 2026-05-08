-- Create cdc_updates table in enginering_alerts schema
CREATE TABLE IF NOT EXISTS enginering_alerts.cdc_updates (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    change_type VARCHAR(10),
    record_id INTEGER,
    title TEXT,
    price NUMERIC,
    location TEXT,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);