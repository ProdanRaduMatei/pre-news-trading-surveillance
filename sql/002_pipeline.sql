CREATE TABLE IF NOT EXISTS market_bars_daily (
  bar_id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  trading_date DATE NOT NULL,
  open DOUBLE NOT NULL,
  high DOUBLE NOT NULL,
  low DOUBLE NOT NULL,
  close DOUBLE NOT NULL,
  volume BIGINT NOT NULL,
  source TEXT NOT NULL,
  ingested_at TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_market_bars_daily_ticker_date
ON market_bars_daily (ticker, trading_date);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  source_event_id TEXT NOT NULL,
  source_table TEXT NOT NULL,
  ticker TEXT NOT NULL,
  issuer_name TEXT NOT NULL,
  first_public_at TIMESTAMP NOT NULL,
  event_date DATE NOT NULL,
  event_type TEXT NOT NULL,
  sentiment_label TEXT NOT NULL,
  sentiment_score DOUBLE NOT NULL,
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  source_url TEXT NOT NULL,
  primary_document TEXT,
  sec_items_json TEXT,
  official_source_flag BOOLEAN NOT NULL,
  timestamp_confidence TEXT NOT NULL,
  classifier_backend TEXT NOT NULL,
  sentiment_backend TEXT NOT NULL,
  novelty_backend TEXT NOT NULL,
  source_quality DOUBLE NOT NULL,
  novelty DOUBLE NOT NULL,
  impact_score DOUBLE NOT NULL,
  built_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS event_market_features_daily (
  event_id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  as_of_date DATE NOT NULL,
  pre_1d_return DOUBLE,
  pre_5d_return DOUBLE,
  pre_20d_return DOUBLE,
  volume_z_1d DOUBLE,
  volume_z_5d DOUBLE,
  volatility_20d DOUBLE,
  gap_pct DOUBLE,
  avg_volume_20d DOUBLE,
  bars_used INTEGER NOT NULL,
  computed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS event_scores (
  event_id TEXT PRIMARY KEY,
  rule_score DOUBLE NOT NULL,
  suspiciousness_score DOUBLE NOT NULL,
  score_band TEXT NOT NULL,
  directional_alignment BOOLEAN NOT NULL,
  explanation_payload TEXT NOT NULL,
  scored_at TIMESTAMP NOT NULL
);
