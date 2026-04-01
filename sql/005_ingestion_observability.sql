ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS duration_ms BIGINT;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS artifact_paths_json TEXT;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS parent_run_id TEXT;
ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS attempt_count INTEGER;
