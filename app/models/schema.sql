-- 1) Vendors + markets
CREATE TABLE IF NOT EXISTS market (
  market_uid        TEXT PRIMARY KEY,      -- vendor-scoped id (e.g., "poly_abc123")
  vendor            TEXT NOT NULL,         -- "polymarket" | "kalshi" | "manifold"
  question          TEXT NOT NULL,
  category          TEXT,
  created_at        TIMESTAMPTZ NOT NULL,
  close_time        TIMESTAMPTZ,
  url               TEXT NOT NULL,
  status            TEXT DEFAULT 'open'    -- open | paused | resolved | closed
);

-- 2) Raw price/volume/liquidity ticks (hourly recommended)
CREATE TABLE IF NOT EXISTS price_tick (
  market_uid        TEXT NOT NULL REFERENCES market(market_uid) ON DELETE CASCADE,
  ts                TIMESTAMPTZ NOT NULL,
  price             NUMERIC,               -- probability (0..1) for Yes-market; choose canonical leg
  volume_24h        NUMERIC,               -- vendor-reported or computed
  liquidity         NUMERIC,
  PRIMARY KEY (market_uid, ts)
);

-- 3) Computed signals (per run)
CREATE TABLE IF NOT EXISTS trend_signal (
  market_uid        TEXT NOT NULL REFERENCES market(market_uid) ON DELETE CASCADE,
  run_at            TIMESTAMPTZ NOT NULL,
  dvol_24h          NUMERIC,               -- (vol_now / vol_24h_ago) - 1
  dprice_24h        NUMERIC,               -- price_now - price_24h_ago
  dliq_24h          NUMERIC,               -- (liq_now / liq_24h_ago) - 1
  freshness         NUMERIC,               -- exp(-age_hours/72)
  PRIMARY KEY (market_uid, run_at)
);

-- 4) Ranked trend scores (per run)
CREATE TABLE IF NOT EXISTS trend_score (
  market_uid        TEXT NOT NULL REFERENCES market(market_uid) ON DELETE CASCADE,
  run_at            TIMESTAMPTZ NOT NULL,
  trend_score       NUMERIC NOT NULL,      -- 0..100
  rank              INTEGER,
  category          TEXT,
  vendor            TEXT,
  dvol_24h          NUMERIC,
  dprice_24h        NUMERIC,
  dliq_24h          NUMERIC,
  price_now         NUMERIC,
  volume_24h_now    NUMERIC,
  liquidity_now     NUMERIC,
  PRIMARY KEY (market_uid, run_at)
);

-- 5) Category momentum (daily aggregates)
CREATE TABLE IF NOT EXISTS trend_category_index (
  category          TEXT NOT NULL,
  date              DATE NOT NULL,
  momentum_7d       NUMERIC,
  avg_dvol_24h      NUMERIC,
  avg_dprice_24h    NUMERIC,
  PRIMARY KEY (category, date)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_price_tick_ts ON price_tick (ts DESC);
CREATE INDEX IF NOT EXISTS idx_trend_score_run_at ON trend_score (run_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_vendor ON market (vendor, status);
