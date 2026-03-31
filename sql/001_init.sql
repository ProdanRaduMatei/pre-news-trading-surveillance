CREATE TABLE IF NOT EXISTS raw_ticker_reference (
  ticker TEXT PRIMARY KEY,
  cik TEXT NOT NULL,
  company_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  retrieved_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_filings (
  filing_id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  cik TEXT NOT NULL,
  company_name TEXT NOT NULL,
  accession_no TEXT NOT NULL,
  form_type TEXT NOT NULL,
  filing_date DATE,
  accepted_at TIMESTAMP,
  items_json TEXT,
  primary_document TEXT,
  primary_doc_description TEXT,
  source_url TEXT NOT NULL,
  raw_path TEXT NOT NULL,
  ingested_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
  run_id TEXT PRIMARY KEY,
  pipeline_name TEXT NOT NULL,
  status TEXT NOT NULL,
  row_count BIGINT NOT NULL,
  metadata_json TEXT,
  started_at TIMESTAMP NOT NULL
);
