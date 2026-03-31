CREATE TABLE IF NOT EXISTS market_bars_minute (
  bar_id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  bar_start TIMESTAMP NOT NULL,
  trading_date DATE NOT NULL,
  open DOUBLE NOT NULL,
  high DOUBLE NOT NULL,
  low DOUBLE NOT NULL,
  close DOUBLE NOT NULL,
  volume BIGINT NOT NULL,
  source TEXT NOT NULL,
  ingested_at TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_market_bars_minute_ticker_bar_start
ON market_bars_minute (ticker, bar_start);

CREATE TABLE IF NOT EXISTS event_market_features_minute (
  event_id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  as_of_timestamp TIMESTAMP NOT NULL,
  pre_15m_return DOUBLE,
  pre_60m_return DOUBLE,
  pre_240m_return DOUBLE,
  volume_z_15m DOUBLE,
  volume_z_60m DOUBLE,
  realized_vol_60m DOUBLE,
  range_pct_60m DOUBLE,
  last_bar_at TIMESTAMP,
  bars_used INTEGER NOT NULL,
  computed_at TIMESTAMP NOT NULL
);
