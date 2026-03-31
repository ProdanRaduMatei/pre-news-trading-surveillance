# Pre-News Trading Surveillance

## FAANG-Level MVP Specification

## Document Purpose

This document defines the MVP for a public-facing, production-ready system that ranks unusual pre-disclosure trading activity around finance-related news and official corporate events.

The goal is to build a system that is:

- technically rigorous
- deployable for public access
- honest about the limits of public data
- architected like a real product, not a notebook project

## Executive Summary

The product will ingest official disclosures and finance-relevant public news, build a canonical event timeline for covered equities, compute NLP and market-based features, and assign a suspiciousness score to each event based on how unusual pre-event trading appears.

The public product should be framed as:

"A research platform for unusual pre-disclosure market activity."

It should not be framed as:

"A public insider-trading accusation engine."

That distinction matters for product quality, legal safety, and credibility.

The MVP will support:

- offline backfills and daily scoring
- a local-first development workflow
- cloud deployment for public access
- analyst-style case pages with explanations
- reproducible modeling and evaluation

## Product Thesis

Public data does not reveal trade ownership in real time, so the system cannot prove illegal insider trading. What public data can do well is identify event-level patterns that look atypical relative to historical norms.

The core product question is:

"For a given public event, was the pre-disclosure trading behavior unusual enough to warrant attention?"

This is strong enough to build a compelling product, publish research, and demonstrate FAANG-level engineering and ML judgment.

## Product Positioning

### Internal Framing

The internal system is a surveillance ranking engine for unusual pre-event activity.

### Public Framing

The public system is a research dashboard showing:

- unusual pre-disclosure market activity
- event timelines
- score explanations
- historical case studies

### Public Safety Constraint

For public release, the safest MVP is not a live accusation feed. It should be a delayed and carefully worded research product.

Recommended public release mode:

- T+1 refresh or end-of-day refresh
- score bands such as `Low`, `Medium`, `High`, not sensational labels
- language like `unusual pre-disclosure activity`
- a permanent disclaimer that scores do not indicate illegality

## Users

### Primary User

- retail or professional market researcher who wants to study unusual pre-event trading behavior

### Secondary User

- recruiter, hiring manager, or technical reviewer assessing the engineering and ML quality of the project

### Future User

- compliance analyst or investigative researcher using the platform as a triage tool

## North-Star Outcome

Deliver a public product that can ingest events, score them reproducibly, and surface the most compelling cases with transparent explanations and measurable model quality.

## MVP Success Metrics

### Product Metrics

- daily batch refresh completes successfully on at least 99 percent of scheduled runs
- event pages load with p95 latency below 300 ms from serving storage
- at least 90 percent of displayed events have complete explanation payloads

### Data Quality Metrics

- at least 95 percent of official-source events have valid canonical timestamps
- duplicate event merge precision above 95 percent on reviewed samples
- ticker resolution precision above 98 percent for covered tickers on reviewed samples

### Model Metrics

- statistically significant lift over rule-only baseline on held-out time-based test data
- `Precision@25` and `Precision@100` materially above matched-random baseline
- full model outperforms market-only and NLP-only ablations

### Public Product Metrics

- zero use of legally risky language in public UI copy
- every public score is backed by deterministic features and a stored explanation payload
- every public event page displays limitations and methodology links

## Non-Goals

- identifying the actual trader or institution
- proving criminal intent or illegality
- real-time broker surveillance
- training large language models or transformers from scratch
- publishing unsupported claims about any person or company

## Core Principles

- build a ranking system, not a prosecutor
- prioritize timestamp quality over model complexity
- preserve raw source data unchanged
- separate offline analytics from online serving
- make the public product conservative and explainable
- ensure every pipeline is reproducible and idempotent

## MVP Scope

### Universe

- U.S. equities
- initial coverage: S&P 500

### Event Sources

- SEC EDGAR filings, especially `8-K`, `6-K`, and relevant exhibits
- official company earnings releases
- company investor-relations press releases
- selected finance news feeds if available under acceptable licensing

### Market Data

- daily OHLCV
- minute OHLCV

### Event Types

- earnings beat or miss
- guidance raise or cut
- M&A announcement or termination
- legal or regulatory action
- executive change
- financing or capital raise
- major contract, product, or sector-specific catalyst

### Public Product Features

- home page with top unusual events
- filters by ticker, date, event type, and score band
- event detail page with timeline, chart, and explanation
- methodology page
- limitations and disclaimer page
- historical case-study pages

## Public Release Strategy

If this product is meant for public use, the recommended MVP has two modes.

### Mode 1: Internal Research Mode

- full numeric scores
- richer event metadata
- experimental views
- review labels and debugging fields

### Mode 2: Public Research Mode

- delayed refresh
- simplified score bands
- conservative wording
- no raw accusation-style phrasing
- no unsupported claims about individuals

This split is an important FAANG-level product decision. It protects trust while still making the work public.

## Functional Requirements

### Event Ingestion

- ingest official disclosures and public news
- preserve raw payloads and metadata
- normalize all timestamps to UTC
- track source provenance

### Canonical Event Construction

- resolve ticker and issuer
- deduplicate related stories
- assign a canonical `event_id`
- compute `first_public_at`
- compute `timestamp_confidence`

### NLP Enrichment

- classify sentiment
- classify event type
- compute novelty
- compute source-quality prior

### Market Analytics

- generate pre-event windows
- compute return, volume, and volatility anomalies
- compute market-relative and sector-relative residual features
- support later options features without schema redesign

### Scoring

- compute deterministic rule score
- compute anomaly score
- compute final suspiciousness score
- generate explanation payload

### Serving

- expose top events for public browsing
- expose event detail data
- expose methodology and limitations pages
- support daily refresh from offline scoring outputs

## Non-Functional Requirements

- all jobs are idempotent
- all transformations are reproducible from raw data
- online serving path uses precomputed scores, not live model inference
- raw, curated, and serving layers are versioned
- deployment supports rollback
- public UI is read-heavy and cheap to serve

## System Architecture

```text
                +----------------------+
                |  SEC / PR / News     |
                +----------+-----------+
                           |
                           v
                +----------------------+
                |   Ingestion Jobs     |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | Raw Storage          |
                | Parquet / Object     |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | Normalize + Resolve  |
                | Canonical Events     |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | NLP Enrichment       |
                | FinBERT + Embeddings |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | Market Feature Jobs  |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | Scoring Pipeline     |
                | Rules + IF + LGBM    |
                +----------+-----------+
                           |
                           v
                +----------------------+
                | Serving Store        |
                | Postgres / Cache     |
                +----------+-----------+
                           |
               +-----------+------------+
               |                        |
               v                        v
     +-------------------+    +-------------------+
     | Public Web App    |    | Internal Review   |
     | Read-Optimized    |    | Diagnostics UI    |
     +-------------------+    +-------------------+
```

## Offline vs Online Architecture

This separation is critical.

### Offline Layer

- ingestion
- normalization
- canonicalization
- NLP enrichment
- feature engineering
- training
- backfills
- batch scoring

### Online Layer

- serves precomputed event scores
- reads from serving database
- renders event pages and ranked lists
- does not depend on slow inference or heavy analytics at request time

This design keeps public latency low and reduces operational risk.

## Recommended Deployment Architecture

### Development

- local Python environment
- local DuckDB
- local Parquet datasets
- optional local Ollama for structured extraction and explanation rewriting

### Public MVP Deployment

- frontend: `Next.js` app
- backend API: `FastAPI` service
- batch workers: Python container jobs
- serving database: managed Postgres
- raw and curated storage: S3-compatible object storage
- cache and rate limiting: Redis-compatible store
- analytics and training artifacts: Parquet plus object storage

### Why This Split Works

- Python remains the best fit for NLP, features, and model training
- Next.js gives a polished public UI and strong deployment ergonomics
- Postgres is sufficient for serving top events and event detail pages
- object storage keeps raw and historical data cheap and reproducible

## Suggested Public Deployment Topology

### Web Tier

- stateless web app
- CDN-backed static assets
- server-side rendering for public event pages

### API Tier

- read-only public endpoints
- internal admin endpoints behind auth
- rate limiting and request logging

### Batch Tier

- scheduled ingestion jobs
- daily canonicalization
- daily feature refresh
- daily score publication job

### Storage Tier

- object store for raw and curated data
- Postgres for serving tables
- optional Redis for caching hot queries

## Public Product Safety Requirements

- use `unusual pre-disclosure activity`, not `insider trading detected`
- publish methodology and limitations prominently
- avoid naming individuals unless sourced from public enforcement documents in a historical case-study section
- delay public refresh if necessary to reduce misuse risk
- store audit logs showing how each public score was produced

## Technology Stack

### Core Data

- `DuckDB`
- Parquet
- Python

### NLP

- `FinBERT` for finance sentiment
- `spaCy` for NER support and text preprocessing
- `sentence-transformers` for similarity and novelty
- optional `Ollama` for local structured extraction or explanation rewriting

### Modeling

- `scikit-learn` for `IsolationForest`
- `LightGBM` for ranking

### Serving

- `FastAPI`
- `Next.js`
- Postgres

### Observability

- structured logs
- metrics
- traces
- error reporting

## Repository Structure

```text
pre-news-trading-surveillance/
  MVP.md
  README.md
  docs/
    ARCHITECTURE.md
    DATA_SCHEMA.md
    MODELING.md
    DEPLOYMENT.md
    RISK_AND_LIMITATIONS.md
  src/
    ingest/
    normalize/
    resolve/
    events/
    nlp/
    features/
    labels/
    models/
    scoring/
    publish/
    api/
    ui/
  configs/
  sql/
  tests/
  scripts/
  notebooks/
  reports/
  data/
    raw/
    bronze/
    silver/
    gold/
```

## Data Layers

### Raw Layer

Immutable landing zone.

- `raw_news`
- `raw_filings`
- `raw_market_daily`
- `raw_market_minute`

### Bronze Layer

Normalized records with source metadata preserved.

### Silver Layer

Entity-resolved and deduplicated event candidates.

### Gold Layer

Serving-ready canonical events, features, labels, and scores.

## Core Data Contracts

### `raw_news`

- `news_id`
- `source`
- `published_at`
- `retrieved_at`
- `title`
- `body`
- `url`
- `source_type`

### `raw_filings`

- `filing_id`
- `accession_no`
- `cik`
- `ticker`
- `form_type`
- `accepted_at`
- `filed_at`
- `document_text`
- `source_url`

### `entities`

- `entity_id`
- `source_record_id`
- `source_table`
- `ticker`
- `company_name`
- `resolver_method`
- `entity_confidence`

### `events`

- `event_id`
- `ticker`
- `issuer_name`
- `first_public_at`
- `primary_source_id`
- `primary_source_table`
- `event_type`
- `official_source_flag`
- `timestamp_confidence`
- `title`
- `summary`
- `sentiment_pos`
- `sentiment_neg`
- `sentiment_neutral`
- `novelty`
- `source_quality`
- `impact_prior`

### `event_windows`

- `event_id`
- `window_name`
- `start_at`
- `end_at`
- `market_session_type`

### `event_features`

- `event_id`
- `car_5d`
- `car_1d`
- `car_60m`
- `resid_ret_5d`
- `resid_ret_1d`
- `resid_ret_60m`
- `vol_z_5d`
- `vol_z_1d`
- `vol_z_60m`
- `minute_of_day_vol_z`
- `rv_shock_1d`
- `rv_shock_60m`
- `gap_pct`
- `sector_relative_move`
- `market_relative_move`
- `dollar_vol_percentile`
- `trend_concentration_60m`

### `scores`

- `event_id`
- `rule_score`
- `anomaly_score`
- `ranker_score`
- `final_score`
- `score_band`
- `explanation_payload`
- `published_at`

### `analyst_reviews`

- `review_id`
- `event_id`
- `label`
- `reason_code`
- `notes`
- `reviewed_at`

## Canonical Event Pipeline

Timestamp discipline is the foundation of the project. A sophisticated model on bad event times is a bad system.

### Step 1: Ingest

- fetch source documents
- store raw payloads unchanged
- normalize to UTC
- record source provenance

### Step 2: Clean

- strip HTML and boilerplate
- preserve title, lead, body, and source metadata
- validate parsing completeness

### Step 3: Resolve Ticker

- use issuer dictionary and alias tables
- use NER as a support signal
- persist resolver confidence and method

### Step 4: Deduplicate

- cluster stories by ticker, time proximity, and embedding similarity
- preserve earliest trustworthy source as canonical
- attach secondary stories as supporting evidence

### Step 5: Create Canonical Event

- assign `event_id`
- compute `first_public_at`
- attach `timestamp_confidence`
- attach `official_source_flag`

### Step 6: Publish Curated Event

- write canonical event to gold layer
- trigger NLP and feature pipelines

## NLP Stack

### Sentiment

Use `FinBERT` on title plus lead text.

Output:

- positive probability
- negative probability
- neutral probability

### Event Typing

Two-stage design:

- v1 rule-based classifier using form types, patterns, and source templates
- v1.1 supervised classifier using reviewed labels

### Novelty

Use sentence embeddings to measure whether an event is truly new relative to recent same-ticker content.

### Source Quality

Assign a prior to each event based on source quality:

- SEC accepted timestamp
- official issuer release
- trusted newswire
- secondary summary article

### Optional Ollama Use

Ollama is allowed only in support roles:

- local structured extraction
- explanation rewriting
- event summary cleanup

It should not sit on the critical scoring path.

## Market Feature Engine

The market feature engine should measure how abnormal trading looked before the event was public.

### Event Windows

- `pre_5d`: `[-5d, -1d]`
- `pre_1d`: `[-1d, -60m]`
- `pre_60m`: `[-60m, -1m]`

Optional validation windows:

- `post_1h`
- `post_1d`

### Core Features

- cumulative abnormal return
- residual return versus market index
- residual return versus sector ETF
- windowed volume z-score
- minute-of-day adjusted volume z-score
- realized volatility shock
- overnight gap percentage
- final-hour trend concentration
- liquidity-normalized dollar volume features

### Baseline Logic

All z-scores should be stock-specific. Intraday features should be adjusted by minute-of-day. Cross-sectional comparisons should be bounded by liquidity buckets to avoid over-flagging thinly traded names.

## Options Layer for v1.5

Planned fields:

- abnormal call volume
- abnormal put volume
- skew shift
- implied volatility jump
- open-interest change
- near-expiry concentration
- out-of-the-money concentration

The schema should reserve room for these features even if v1 launches without them.

## Modeling Strategy

The scoring system should be layered for interpretability and robustness.

### Layer 1: Rule-Based Suspicion Score

Captures obvious cases:

- strong directional drift before aligned event
- high pre-event volume spike
- high novelty
- high source confidence

### Layer 2: Unsupervised Market Anomaly Model

Recommended model:

- `IsolationForest`

Purpose:

- detect rare multivariate pre-event market patterns

### Layer 3: Weakly Supervised Ranking Model

Recommended model:

- `LightGBM`

Inputs:

- NLP features
- event metadata
- market anomaly features
- anomaly score
- rule score

Output:

- final suspiciousness score in a bounded range

This output is a triage ranking score, not a probability of criminal behavior.

## Labels and Ground Truth Strategy

The product should be explicit that labels are weak and partial.

### Positive Seed Set

Use public historical enforcement material:

- SEC litigation releases
- insider-trading complaint summaries
- historical case-study documents

Map each case to:

- ticker
- event date
- approximate public disclosure window

### Control Set

Construct matched controls by:

- sector
- market cap bucket
- event type
- time period

### Unknown Set

Most events remain unknown. That is acceptable for a ranking system and should be documented.

## Evaluation Framework

This is one of the main differences between a solid project and a FAANG-level one.

### Split Policy

- use only time-based train, validation, and test splits
- prohibit random splits across time

### Primary Metrics

- `Precision@25`
- `Precision@100`
- top-decile lift versus random baseline
- `NDCG`

### Secondary Metrics

- performance by event type
- performance by sector
- performance by liquidity bucket
- feature drift over time
- score stability by quarter

### Required Ablations

- rules only
- market features only
- NLP features only
- market plus NLP
- market plus NLP plus anomaly model
- full ranker

### Human Review Loop

For every major run:

- review the top-ranked events
- assign error categories
- update label store
- measure recurring failure modes

Typical failure modes:

- bad timestamp
- event rumor already public
- macro or sector move
- earnings anticipation
- poor ticker resolution

## Explainability Requirements

Every public score must have an explanation payload generated during scoring, not improvised at request time.

Minimum explanation components:

- top triggering features
- event metadata summary
- source quality
- timestamp confidence
- novelty level

Example:

"This event was ranked highly because trading volume in the 60 minutes before disclosure was 4.8 standard deviations above baseline, price drift was strongly positive relative to the sector ETF, and the event was classified as a high-novelty M&A disclosure from an official source."

## Public API Requirements

### Read Endpoints

- `GET /events`
- `GET /events/{event_id}`
- `GET /events/top`
- `GET /tickers/{ticker}/events`
- `GET /methodology`
- `GET /health`

### Internal Endpoints

- `POST /jobs/score-daily`
- `POST /jobs/backfill`
- `POST /reviews`

### Serving Rule

Public API should read from precomputed serving tables only.

## UI Requirements

### Public Web App

- landing page with score distribution and recent unusual events
- ranked event table
- ticker pages
- event detail pages
- methodology page
- disclaimer page

### Internal Review UI

- score debugging fields
- feature inspection
- raw source documents
- analyst review tools

## Observability

To reach FAANG-level quality, the system needs operational visibility.

### Pipeline Monitoring

- ingestion success rate
- canonicalization failure counts
- missing timestamp counts
- feature-generation failures
- daily score publication counts

### Model Monitoring

- score distribution drift
- feature drift
- missing-feature rates
- model version usage

### Product Monitoring

- page latency
- API error rate
- cache hit rate
- public traffic patterns

## Testing Strategy

### Unit Tests

- timestamp normalization
- ticker resolution
- event-window creation
- feature calculations

### Integration Tests

- source ingest to canonical event
- canonical event to feature row
- feature row to serving score

### Data Quality Tests

- no null canonical timestamps for publishable events
- no duplicate active canonical events for same source cluster
- score payload exists for every published event

### Regression Tests

- stable scoring on known benchmark events
- no major unexplained drift across model releases

## Security and Abuse Prevention

### Public Surface

- read-only public endpoints
- rate limiting
- bot protection if needed
- structured request logging

### Internal Surface

- admin auth
- restricted review tools
- restricted backfill and publish actions

### Legal and Trust Guardrails

- methodology and disclaimers must be visible
- no unsupported allegations in UI copy
- preserve audit trail of score generation

## Public Deployment Readiness Checklist

- public copy reviewed for legal-risk language
- delay policy defined
- methodology page published
- disclaimer page published
- score explanations available
- read-only public API deployed
- admin actions gated behind auth
- monitoring and alerting configured

## Build Plan

### Phase 1: Data Foundation

- create repo structure
- set up local data lake with DuckDB and Parquet
- ingest SEC and official issuer events
- ingest daily and minute market data

### Phase 2: Canonical Event Engine

- issuer dictionary and alias mapping
- ticker resolution
- event dedupe
- canonical event builder
- timestamp confidence scoring

### Phase 3: NLP Enrichment

- `FinBERT` sentiment
- rule-based event typing
- embedding-based novelty
- source-quality priors

### Phase 4: Market Feature Engine

- event-window generation
- residual return features
- abnormal volume features
- realized volatility features
- baseline rule score

### Phase 5: Modeling and Evaluation

- `IsolationForest`
- weak-label pipeline
- `LightGBM` ranker
- backtest framework
- ablation runs

### Phase 6: Public Serving Layer

- serving tables in Postgres
- read API
- public web app
- methodology and disclaimer pages

### Phase 7: Operations

- batch scheduler
- observability
- alerts
- rollback process

## Definition of Done for MVP

The MVP is complete when all of the following are true:

- two or more years of S&P 500 event history are ingested and reproducible
- canonical events are generated with stored timestamp confidence
- NLP, market, and anomaly features are available in gold tables
- every publishable event has a score and explanation payload
- the public web app can serve ranked events and event detail pages
- the system runs on a scheduled daily refresh
- a full time-split backtest and ablation report has been completed
- methodology, limitations, and disclaimers are publicly available

## What Makes This FAANG-Level

This project becomes FAANG-level if it demonstrates:

- strong problem framing
- data contracts and lineage
- careful timestamp and dedupe handling
- offline and online separation
- measurable evaluation
- explainable ranking
- production deployment thinking
- observability and rollback planning
- honest product and legal constraints

## Recommended Next Documents

- `README.md`
- `docs/ARCHITECTURE.md`
- `docs/DATA_SCHEMA.md`
- `docs/MODELING.md`
- `docs/DEPLOYMENT.md`
- `docs/RISK_AND_LIMITATIONS.md`

## References

- FinBERT: https://huggingface.co/ProsusAI/finbert
- Hugging Face sequence classification docs: https://huggingface.co/docs/transformers/en/tasks/sequence_classification
- Sentence Transformers: https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2
- spaCy linguistic features: https://spacy.io/usage/linguistic-features
- scikit-learn IsolationForest: https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html
- DuckDB Parquet docs: https://duckdb.org/docs/stable/data/parquet/overview
- SEC EDGAR API docs: https://www.sec.gov/edgar/sec-api-documentation
- Ollama structured outputs: https://docs.ollama.com/capabilities/structured-outputs
