# Pre-News Trading Surveillance

Pre-News Trading Surveillance is a public-facing research platform for ranking unusual pre-disclosure market activity around official corporate events and finance-related news. The system is designed as a surveillance and triage engine, not a legal accusation engine.

## What Is In Scope

- canonical event building from SEC filings and official issuer disclosures
- NLP enrichment such as finance sentiment, event typing, and novelty
- market anomaly features over pre-event windows
- suspiciousness ranking with explainable outputs
- public serving architecture with conservative product language

## What Is Implemented Today

- FAANG-level MVP specification in [MVP.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/MVP.md)
- core architecture and deployment docs in [docs](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs)
- a runnable SEC ingestion slice that:
  - bootstraps the local data lake and DuckDB database
  - downloads the SEC ticker reference map
  - fetches SEC submissions for requested tickers
  - fetches official issuer press releases and earnings releases from configured RSS/Atom feeds
  - stores raw payloads in `data/raw`
  - writes normalized filing rows to `data/bronze`
  - loads normalized rows into DuckDB tables
- a canonical SEC event builder and rule-based scoring pipeline that:
  - builds publishable event rows from raw SEC filings
  - ingests daily market bars from CSV
  - ingests minute market bars from CSV
  - computes daily pre-event market features
  - computes minute pre-event market features
  - assigns explainable suspiciousness scores
  - serves ranked events from a public dashboard plus JSON API

## Project Layout

```text
pre-news-trading-surveillance/
  docs/
  configs/
  data/
  sql/
  src/pre_news_trading_surveillance/
  tests/
  MVP.md
  README.md
  pyproject.toml
```

## Quickstart

```bash
cd /Users/matei/AIFinanceAssistent/pre-news-trading-surveillance
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

For optional fully local NLP backends such as FinBERT sentiment and embedding-based novelty:

```bash
pip install -e .[nlp]
```

For the anomaly-stack scorer with `IsolationForest` and `LightGBM`:

```bash
pip install -e .[ml]
```

Initialize the local database and directory structure:

```bash
pnts bootstrap
```

Fetch the SEC ticker reference snapshot:

```bash
pnts ingest-sec-reference --user-agent "Your Name your-email@example.com"
```

Fetch recent SEC submissions for a starter ticker set:

```bash
pnts ingest-sec-filings \
  --user-agent "Your Name your-email@example.com" \
  --tickers AAPL MSFT NVDA \
  --forms 8-K 6-K
```

Ingest official issuer press releases or earnings releases from configured feeds:

```bash
pnts ingest-press-releases --config configs/issuer_feeds.example.toml --tickers AAPL MSFT
```

Import daily market bars from a CSV with headers `ticker,date,open,high,low,close,volume`:

```bash
pnts ingest-market-daily --csv /absolute/path/to/market_daily.csv
```

Import minute market bars from a CSV with headers `ticker,timestamp,open,high,low,close,volume`:

```bash
pnts ingest-market-minute --csv /absolute/path/to/market_minute.csv
```

Or pull market data directly from Alpha Vantage:

```bash
export ALPHAVANTAGE_API_KEY="your-key"
pnts ingest-market-daily --provider alpha_vantage --tickers AAPL MSFT --outputsize compact
pnts ingest-market-minute --provider alpha_vantage --tickers AAPL MSFT --interval 1min --outputsize compact
```

Optional intraday flags include `--month YYYY-MM`, `--entitlement delayed|realtime`, `--adjusted`, and `--no-extended-hours`.

Build canonical SEC events, compute daily and minute features, and score them:

```bash
pnts build-sec-events --forms 8-K 6-K
pnts compute-daily-features
pnts compute-minute-features
pnts score-events
```

Train the anomaly stack and then score with the hybrid engine:

```bash
pnts train-model-stack --min-samples 12
pnts score-events --engine hybrid
```

Export a manual benchmark review queue, import reviewed labels, and run the historical backtest:

```bash
pnts export-benchmark-candidates --output-csv data/benchmarks/review_candidates.csv --top-k 50 --bottom-k 50
pnts import-benchmark-labels --csv data/benchmarks/reviewed_benchmark.csv --reviewer "Your Name"
pnts run-backtest --folds 3 --min-train-size 24 --k-values 5 10 25
```

Inspect recent ingestion, feature, scoring, and publish runs:

```bash
pnts list-ingestion-runs --limit 20
```

Run the orchestrated refresh pipeline from config:

```bash
export SEC_USER_AGENT="Your Name your-email@example.com"
export ALPHAVANTAGE_API_KEY="your-key"
pnts refresh-pipeline --config configs/refresh_pipeline.example.toml --mode full
```

Build and optionally upload a public snapshot bundle directly:

```bash
pnts publish-snapshot --events-limit 250 --output-dir data/publish/current
```

If you already have local models on disk, you can switch the SEC event builder to richer on-device NLP backends:

```bash
pnts build-sec-events \
  --forms 8-K 6-K \
  --sentiment-backend finbert \
  --sentiment-model /absolute/path/to/finbert \
  --novelty-backend sentence-transformers \
  --novelty-model /absolute/path/to/all-MiniLM-L6-v2
```

Run the local API:

```bash
pnts serve-api --host 127.0.0.1 --port 8000
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000) for the public dashboard or [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) for the API schema.

## Current CLI Commands

- `pnts bootstrap`
- `pnts ingest-sec-reference --user-agent "..."`
- `pnts ingest-sec-filings --user-agent "..." --tickers AAPL MSFT`
- `pnts ingest-press-releases --config configs/issuer_feeds.example.toml --tickers AAPL MSFT`
- `pnts ingest-market-daily --csv /path/to/file.csv`
- `pnts ingest-market-minute --csv /path/to/file.csv`
- `pnts ingest-market-daily --provider alpha_vantage --tickers AAPL MSFT`
- `pnts ingest-market-minute --provider alpha_vantage --tickers AAPL MSFT --interval 1min`
- `pnts build-sec-events --sentiment-backend heuristic|finbert --novelty-backend lexical|sentence-transformers`
- `pnts compute-daily-features`
- `pnts compute-minute-features`
- `pnts train-model-stack --min-samples 12`
- `pnts export-benchmark-candidates --output-csv data/benchmarks/review_candidates.csv`
- `pnts import-benchmark-labels --csv data/benchmarks/reviewed_benchmark.csv`
- `pnts run-backtest --folds 3 --min-train-size 24`
- `pnts score-events`
- `pnts list-ingestion-runs --limit 20`
- `pnts publish-snapshot --output-dir data/publish/current`
- `pnts refresh-pipeline --config configs/refresh_pipeline.example.toml --mode full|intraday`
- `pnts serve-api`

## Provider Notes

- Alpha Vantage daily bars are available through the `TIME_SERIES_DAILY` endpoint.
- Alpha Vantage intraday bars are exposed through `TIME_SERIES_INTRADAY`.
- The repo stores raw provider CSV snapshots under `data/raw/market/alpha_vantage/` before loading normalized rows into DuckDB.
- Local CSV imports are copied into `data/raw/market/csv/` so feature and score batches remain reproducible even when the original source file changes.
- SEC and provider pulls now use bounded retry logic with rate-limit awareness, and each run records attempts, artifacts, and final status in DuckDB.
- Official issuer releases are ingested from configurable RSS/Atom feeds defined in [issuer_feeds.example.toml](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/configs/issuer_feeds.example.toml).

## Public Dashboard

The app now includes a polished public dashboard served directly from FastAPI. It provides:

- top-level coverage, score, and freshness metrics
- breakdowns for score bands, event types, top tickers, and recent activity
- a filterable ranked event feed
- offset-based pagination, short-lived cache headers, and basic rate limiting on public API routes
- an event detail rail with scoring explanation, feature values, source links, and public-policy context

## Scheduled Refresh

- The repo now includes a config-driven orchestrator at `pnts refresh-pipeline`.
- The sample config lives at [configs/refresh_pipeline.example.toml](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/configs/refresh_pipeline.example.toml).
- Official issuer feed config lives at [configs/issuer_feeds.example.toml](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/configs/issuer_feeds.example.toml).
- GitHub Actions scheduling lives at [.github/workflows/refresh.yml](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/.github/workflows/refresh.yml).
- The workflow assumes two repository secrets:
  - `SEC_USER_AGENT`
  - `ALPHAVANTAGE_API_KEY`
- Optional issuer feed settings can also be supplied through:
  - `PNTS_ISSUER_FEED_CONFIG`
  - `PRESS_RELEASES_USER_AGENT`
- Default schedules:
  - weekday hourly intraday refresh at `15` minutes past the hour UTC
  - weekday full refresh at `22:25 UTC`

## Published Snapshot Mode

- The pipeline now produces a public JSON bundle under `data/publish/current`.
- Published bundles are public-safe by default and apply a delayed visibility window before writing `summary.json`, `events.json`, and detail payloads.
- Set `PNTS_API_DATA_SOURCE=published` to make the FastAPI app serve from the published bundle instead of DuckDB.
- Override the bundle location with `PNTS_PUBLISHED_DATA_DIR=/absolute/path/to/published/bundle`.
- Public serving safeguards are controlled with:
  - `PNTS_PUBLIC_SAFE_MODE=true|false`
  - `PNTS_PUBLIC_DELAY_MINUTES=1440`
  - `PNTS_RATE_LIMIT_MAX_REQUESTS=120`
  - `PNTS_RATE_LIMIT_WINDOW_SECONDS=60`
- Optional object-storage upload is supported through S3-compatible settings in the refresh config and GitHub secrets such as:
  - `PUBLISH_S3_BUCKET`
  - `PUBLISH_S3_REGION`
  - `PUBLISH_S3_ENDPOINT_URL`
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`

## Public Deployment

- The repository is ready for Vercel deployment through the root [app.py](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/app.py) ASGI entrypoint and [vercel.json](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/vercel.json).
- Recommended production env vars:
  - `PNTS_API_DATA_SOURCE=published`
  - `PNTS_PUBLISHED_DATA_DIR=/var/task/data/publish/current`
  - `PNTS_PUBLIC_SAFE_MODE=true`
  - `PNTS_PUBLIC_DELAY_MINUTES=1440`
  - `PNTS_RATE_LIMIT_MAX_REQUESTS=120`
  - `PNTS_RATE_LIMIT_WINDOW_SECONDS=60`
- The public experience is intentionally framed as delayed research triage. It should not expose `/ingestion-runs` or any internal operator surface.

## Reproducibility And Run Visibility

- Every tracked command now writes a `running` -> `success|failed` lifecycle row into `ingestion_runs`.
- Run metadata includes retry counts, upstream lineage, and immutable artifact paths for raw ingests, feature snapshots, scoring inputs, and score outputs.
- The API exposes internal run visibility at `/ingestion-runs` when serving directly from DuckDB.
- Trained anomaly-stack artifacts live under `data/models/scoring/current/` and include a `manifest.json` plus a pickled model bundle.
- Reviewed benchmark labels live in DuckDB under `benchmark_event_labels`, and evaluation reports are written to `reports/evaluation/`.

## Evaluation And Credibility

- Manual review queues can be exported to `data/benchmarks/review_candidates.csv`.
- Use [reviewed_benchmark.template.csv](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/data/benchmarks/reviewed_benchmark.template.csv) as the starter schema for reviewed suspicious/control labels.
- Historical backtests report `Precision@K`, top-decile lift, and ablations across rule, NLP-only, market-only, anomaly-only, and hybrid engines.
- Full workflow details live in [EVALUATION.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/EVALUATION.md).

## Core Docs

- [MVP.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/MVP.md)
- [ARCHITECTURE.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/ARCHITECTURE.md)
- [DATA_SCHEMA.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/DATA_SCHEMA.md)
- [MODELING.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/MODELING.md)
- [EVALUATION.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/EVALUATION.md)
- [DEPLOYMENT.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/DEPLOYMENT.md)
- [RISK_AND_LIMITATIONS.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/RISK_AND_LIMITATIONS.md)

## GitHub

The standalone repository is published at [github.com/ProdanRaduMatei/pre-news-trading-surveillance](https://github.com/ProdanRaduMatei/pre-news-trading-surveillance).
