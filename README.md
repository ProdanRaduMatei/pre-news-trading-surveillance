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

Import daily market bars from a CSV with headers `ticker,date,open,high,low,close,volume`:

```bash
pnts ingest-market-daily --csv /absolute/path/to/market_daily.csv
```

Import minute market bars from a CSV with headers `ticker,timestamp,open,high,low,close,volume`:

```bash
pnts ingest-market-minute --csv /absolute/path/to/market_minute.csv
```

Build canonical SEC events, compute daily and minute features, and score them:

```bash
pnts build-sec-events --forms 8-K 6-K
pnts compute-daily-features
pnts compute-minute-features
pnts score-events
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
- `pnts ingest-market-daily --csv /path/to/file.csv`
- `pnts ingest-market-minute --csv /path/to/file.csv`
- `pnts build-sec-events --sentiment-backend heuristic|finbert --novelty-backend lexical|sentence-transformers`
- `pnts compute-daily-features`
- `pnts compute-minute-features`
- `pnts score-events`
- `pnts serve-api`

## Public Dashboard

The app now includes a polished public dashboard served directly from FastAPI. It provides:

- top-level coverage, score, and freshness metrics
- breakdowns for score bands, event types, top tickers, and recent activity
- a filterable ranked event feed
- an event detail rail with scoring explanation, feature values, and source links

## Core Docs

- [MVP.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/MVP.md)
- [ARCHITECTURE.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/ARCHITECTURE.md)
- [DATA_SCHEMA.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/DATA_SCHEMA.md)
- [MODELING.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/MODELING.md)
- [DEPLOYMENT.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/DEPLOYMENT.md)
- [RISK_AND_LIMITATIONS.md](/Users/matei/AIFinanceAssistent/pre-news-trading-surveillance/docs/RISK_AND_LIMITATIONS.md)

## GitHub

The standalone repository is published at [github.com/ProdanRaduMatei/pre-news-trading-surveillance](https://github.com/ProdanRaduMatei/pre-news-trading-surveillance).
