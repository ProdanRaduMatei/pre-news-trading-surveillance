# Deployment

## Goal

Deploy a public-facing research product with a conservative serving model and a separate offline analytics pipeline.

## Recommended MVP Deployment

### API

- `FastAPI` service
- read-only public endpoints
- authenticated internal admin endpoints

### Data and Storage

- Postgres for serving tables
- object storage for raw and curated artifacts
- Redis for cache and rate limits if needed

### Batch Jobs

- containerized Python jobs
- daily ingestion
- daily feature refresh
- daily score publication
- config-driven orchestration via `pnts refresh-pipeline`
- cron or scheduler integration for intraday and full refresh modes

## Public MVP Shape

The repo now supports a single-service public deployment:

- Vercel-hosted `FastAPI` app and dashboard via [app.py](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/app.py)
- public-safe published bundle serving from `data/publish/current`
- scheduled refresh pipeline producing delayed bundles
- optional S3-compatible upload for external distribution or backup

## Release Model

For public launch, publish:

- delayed refreshes such as T+1 or end-of-day
- score bands rather than sensational claims
- explicit methodology and limitations pages
- a disclaimer on every event page
- rate-limited read-only endpoints with no operational run visibility

## CI/CD

The project includes a starter GitHub Actions workflow in [.github/workflows/ci.yml](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/.github/workflows/ci.yml).

Scheduled refresh automation now lives in [.github/workflows/refresh.yml](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/.github/workflows/refresh.yml). It supports:

- weekday hourly intraday refresh
- weekday full refresh
- manual `workflow_dispatch` for either mode
- published snapshot generation on every refresh run
- optional S3-compatible upload for the published bundle

For a standalone repository, extend it with:

- lint and formatting checks
- container builds
- deployment previews
- migration verification

## Operational Requirements

- rollbackable releases
- schema migration discipline
- request logging on public endpoints
- monitoring on ingestion, scoring, and serving freshness

## Environment Contracts

The scheduled refresh workflow assumes:

- `SEC_USER_AGENT` secret for SEC-compliant requests
- `ALPHAVANTAGE_API_KEY` secret for market data pulls
- optional publish secrets:
  - `PUBLISH_S3_BUCKET`
  - `PUBLISH_S3_REGION`
  - `PUBLISH_S3_ENDPOINT_URL`
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_SESSION_TOKEN`
- refresh config checked into the repo, with secrets injected through environment variables rather than hardcoded in config
- public app env:
  - `PNTS_API_DATA_SOURCE=published`
  - `PNTS_PUBLISHED_DATA_DIR=/var/task/data/publish/current`
  - `PNTS_PUBLIC_SAFE_MODE=true`
  - `PNTS_PUBLIC_DELAY_MINUTES=1440`
  - `PNTS_RATE_LIMIT_MAX_REQUESTS=120`
  - `PNTS_RATE_LIMIT_WINDOW_SECONDS=60`

## Public Data Serving Modes

The FastAPI app can now serve from two backends:

- `duckdb` mode:
  - reads directly from the local analytical DuckDB
- `published` mode:
  - reads from a curated snapshot bundle generated under `data/publish/current` or another mounted path

Set the deployed app environment like this for published mode:

- `PNTS_API_DATA_SOURCE=published`
- `PNTS_PUBLISHED_DATA_DIR=/mounted/public/snapshot/path`
- `PNTS_PUBLIC_SAFE_MODE=true`
- `PNTS_PUBLIC_DELAY_MINUTES=1440`

## Vercel Deployment

The repository includes:

- [app.py](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/app.py) as the ASGI entrypoint
- [vercel.json](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/vercel.json) for function configuration and static cache headers

Recommended deploy posture:

- deploy the app in `published` mode
- keep public-safe mode enabled
- ship only delayed bundles to the public-serving path
- use the GitHub refresh workflow to keep the published bundle fresh

## Recommended Refresh Split

- `intraday` mode:
  - SEC filings
  - minute bars
  - canonical events
  - minute features
  - scores
- `full` mode:
  - SEC reference
  - SEC filings
  - daily bars
  - minute bars
  - canonical events
  - daily features
  - minute features
  - scores
  - published snapshot
