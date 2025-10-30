-- Enable TimescaleDB (safe to re-run)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Promote to hypertables (idempotent)
SELECT create_hypertable('price_tick','ts', if_not_exists => TRUE);
SELECT create_hypertable('trend_score','run_at', if_not_exists => TRUE);

-- Optional: compress older chunks for cheaper storage
ALTER TABLE price_tick SET (timescaledb.compress, timescaledb.compress_segmentby = 'market_uid');
ALTER TABLE trend_score SET (timescaledb.compress, timescaledb.compress_segmentby = 'market_uid');

-- Compression/retention policies (tweak to taste)
-- Keep raw price ticks 90d, but compress after 7d
SELECT add_compression_policy('price_tick', INTERVAL '7 days');
SELECT add_retention_policy('price_tick', INTERVAL '90 days');

-- Keep trend_score 180d, compress after 14d
SELECT add_compression_policy('trend_score', INTERVAL '14 days');
SELECT add_retention_policy('trend_score', INTERVAL '180 days');

-- (Optional) Continuous aggregate for daily category momentum
-- This materializes a daily view to speed up the Category Momentum widget
CREATE MATERIALIZED VIEW IF NOT EXISTS category_momentum_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', ts := run_at) AS day,
  m.category,
  AVG(t.trend_score)            AS avg_trend_score,
  AVG(t.dvol_24h)               AS avg_dvol_24h,
  AVG(t.dprice_24h)             AS avg_dprice_24h
FROM trend_score t
JOIN market m ON m.market_uid = t.market_uid
GROUP BY day, m.category;

-- Refresh policy: keep it current (runs every hour and refreshes last 7 days)
SELECT add_continuous_aggregate_policy(
  'category_momentum_daily',
  start_offset => INTERVAL '7 days',
  end_offset   => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);
