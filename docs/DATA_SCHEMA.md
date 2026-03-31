# Data Schema

## Layering Model

The project uses a four-layer data model:

- `raw`: immutable source payloads
- `bronze`: normalized source records
- `silver`: canonicalized and entity-resolved event candidates
- `gold`: feature-complete and serving-ready tables

## Core Tables

### `raw_ticker_reference`

SEC reference mapping used for ticker to CIK resolution.

- `ticker`
- `cik`
- `company_name`
- `source_url`
- `retrieved_at`

### `raw_filings`

Normalized SEC submission rows derived from issuer submission payloads.

- `filing_id`
- `ticker`
- `cik`
- `company_name`
- `accession_no`
- `form_type`
- `filing_date`
- `accepted_at`
- `primary_document`
- `primary_doc_description`
- `source_url`
- `raw_path`
- `ingested_at`

### `entities`

Planned silver-layer entity resolution output.

- `entity_id`
- `source_record_id`
- `source_table`
- `ticker`
- `company_name`
- `resolver_method`
- `entity_confidence`

### `events`

Planned canonical event table.

- `event_id`
- `ticker`
- `issuer_name`
- `first_public_at`
- `event_type`
- `timestamp_confidence`
- `official_source_flag`
- `title`
- `summary`

### `event_features`

Planned gold-layer feature store.

- `event_id`
- `car_5d`
- `car_1d`
- `car_60m`
- `vol_z_1d`
- `vol_z_60m`
- `rv_shock_1d`
- `rv_shock_60m`
- `novelty`
- `source_quality`

### `scores`

Planned serving score table.

- `event_id`
- `rule_score`
- `anomaly_score`
- `ranker_score`
- `final_score`
- `score_band`
- `explanation_payload`
- `published_at`

## Current SQL Source

The initial database schema lives in [001_init.sql](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/sql/001_init.sql).

That schema currently initializes:

- `raw_ticker_reference`
- `raw_filings`
- `ingestion_runs`

## Storage Conventions

- raw source payloads live under `data/raw`
- normalized NDJSON snapshots live under `data/bronze`
- DuckDB database lives under `data/gold/pnts.duckdb`

## Evolution Strategy

- add new tables through versioned SQL migration files
- never break the meaning of existing fields silently
- keep raw payload locations stable so backfills remain reproducible
