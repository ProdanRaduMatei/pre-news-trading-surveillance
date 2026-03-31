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
  - stores raw payloads in `data/raw`
  - writes normalized filing rows to `data/bronze`
  - loads normalized rows into DuckDB tables

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

## Current CLI Commands

- `pnts bootstrap`
- `pnts ingest-sec-reference --user-agent "..."`
- `pnts ingest-sec-filings --user-agent "..." --tickers AAPL MSFT`

## Core Docs

- [MVP.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/MVP.md)
- [ARCHITECTURE.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/ARCHITECTURE.md)
- [DATA_SCHEMA.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/DATA_SCHEMA.md)
- [MODELING.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/MODELING.md)
- [DEPLOYMENT.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/DEPLOYMENT.md)
- [RISK_AND_LIMITATIONS.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/RISK_AND_LIMITATIONS.md)

## Notes on GitHub

This folder currently lives inside a larger workspace repository rooted at [AIFinanceAssistent](/Users/matei/AIFinanceAssistent). The contents here are structured so they can be promoted into a standalone GitHub repository cleanly, but creating that standalone remote needs one product decision first:

- keep this as a subproject in the existing repository
- or split this folder into its own dedicated repository

The project files are now laid out to support the second option without additional restructuring.
