CREATE TABLE IF NOT EXISTS raw_issuer_releases (
  release_id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  issuer_name TEXT NOT NULL,
  source_name TEXT NOT NULL,
  feed_url TEXT NOT NULL,
  entry_guid TEXT,
  title TEXT NOT NULL,
  summary_text TEXT,
  source_url TEXT NOT NULL,
  published_at TIMESTAMP,
  raw_path TEXT NOT NULL,
  ingested_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_issuer_releases_ticker_published
ON raw_issuer_releases (ticker, published_at);
