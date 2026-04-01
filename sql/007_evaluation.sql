CREATE TABLE IF NOT EXISTS benchmark_event_labels (
  event_id TEXT PRIMARY KEY,
  benchmark_label TEXT NOT NULL,
  review_status TEXT NOT NULL,
  reviewer TEXT,
  label_source TEXT NOT NULL,
  confidence DOUBLE,
  review_notes TEXT,
  metadata_json TEXT,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_benchmark_event_labels_label
ON benchmark_event_labels (benchmark_label, review_status);

CREATE INDEX IF NOT EXISTS idx_benchmark_event_labels_reviewer
ON benchmark_event_labels (reviewer);
