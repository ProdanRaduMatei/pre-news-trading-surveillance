ALTER TABLE raw_filings ADD COLUMN IF NOT EXISTS items_json TEXT;

ALTER TABLE events ADD COLUMN IF NOT EXISTS sec_items_json TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS classifier_backend TEXT DEFAULT 'heuristic';
ALTER TABLE events ADD COLUMN IF NOT EXISTS sentiment_backend TEXT DEFAULT 'heuristic';
ALTER TABLE events ADD COLUMN IF NOT EXISTS novelty_backend TEXT DEFAULT 'heuristic';
