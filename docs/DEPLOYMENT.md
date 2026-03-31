# Deployment

## Goal

Deploy a public-facing research product with a conservative serving model and a separate offline analytics pipeline.

## Recommended MVP Deployment

### Web

- `Next.js` public site
- static assets on CDN
- server-side rendering for event detail pages

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

## Reference Cloud Shape

One pragmatic MVP setup:

- Vercel for the web frontend
- container-hosted Python API and workers on Render, Fly.io, Railway, or Cloud Run
- managed Postgres
- S3-compatible object storage

## Release Model

For public launch, publish:

- delayed refreshes such as T+1 or end-of-day
- score bands rather than sensational claims
- explicit methodology and limitations pages
- a disclaimer on every event page

## CI/CD

The project includes a starter GitHub Actions workflow in [.github/workflows/ci.yml](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/.github/workflows/ci.yml).

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
