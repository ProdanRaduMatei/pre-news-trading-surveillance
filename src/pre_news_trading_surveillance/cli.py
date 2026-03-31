from __future__ import annotations

import argparse
from pathlib import Path

from . import db
from .events import sec_events
from .features import daily as daily_features
from .ingest import sec
from .scoring import rules
from .settings import default_paths


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pnts",
        description="Pre-News Trading Surveillance project utilities.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser("bootstrap", help="Create directories and initialize DuckDB.")
    bootstrap.add_argument(
        "--db-path",
        type=Path,
        help="Optional path override for the DuckDB database.",
    )

    ingest_reference = subparsers.add_parser(
        "ingest-sec-reference",
        help="Download the SEC ticker reference map and load it into storage.",
    )
    ingest_reference.add_argument(
        "--user-agent",
        required=True,
        help="SEC-compatible user agent, for example 'Your Name your-email@example.com'.",
    )
    ingest_reference.add_argument(
        "--skip-db",
        action="store_true",
        help="Only write filesystem artifacts and skip database loading.",
    )

    ingest_filings = subparsers.add_parser(
        "ingest-sec-filings",
        help="Fetch recent SEC submission data for one or more tickers.",
    )
    ingest_filings.add_argument(
        "--user-agent",
        required=True,
        help="SEC-compatible user agent, for example 'Your Name your-email@example.com'.",
    )
    ingest_filings.add_argument(
        "--tickers",
        nargs="+",
        required=True,
        help="Ticker symbols to fetch from the SEC submissions feed.",
    )
    ingest_filings.add_argument(
        "--forms",
        nargs="*",
        default=[],
        help="Optional SEC form types to keep, for example 8-K 6-K.",
    )
    ingest_filings.add_argument(
        "--per-ticker-limit",
        type=int,
        default=50,
        help="Maximum recent filing rows to keep per ticker after filtering.",
    )
    ingest_filings.add_argument(
        "--refresh-reference",
        action="store_true",
        help="Refresh the cached SEC ticker reference snapshot before resolving tickers.",
    )
    ingest_filings.add_argument(
        "--skip-db",
        action="store_true",
        help="Only write filesystem artifacts and skip database loading.",
    )

    ingest_market_daily = subparsers.add_parser(
        "ingest-market-daily",
        help="Load daily market bars from a CSV file into storage.",
    )
    ingest_market_daily.add_argument("--csv", type=Path, required=True, help="CSV path.")
    ingest_market_daily.add_argument(
        "--source",
        default="csv_import",
        help="Source label stored with the imported bars.",
    )

    build_events = subparsers.add_parser(
        "build-sec-events",
        help="Build canonical SEC-backed events from raw filing rows.",
    )
    build_events.add_argument(
        "--forms",
        nargs="*",
        default=["8-K", "6-K"],
        help="Optional SEC form filters.",
    )
    build_events.add_argument(
        "--sentiment-backend",
        default="heuristic",
        help="Sentiment backend: heuristic or finbert.",
    )
    build_events.add_argument(
        "--sentiment-model",
        help="Optional local model path or model name for the sentiment backend.",
    )
    build_events.add_argument(
        "--novelty-backend",
        default="lexical",
        help="Novelty backend: lexical or sentence-transformers.",
    )
    build_events.add_argument(
        "--novelty-model",
        help="Optional local model path or model name for the novelty backend.",
    )

    compute_features = subparsers.add_parser(
        "compute-daily-features",
        help="Compute daily pre-event market features for canonical events.",
    )
    compute_features.add_argument(
        "--ticker",
        help="Optional single-ticker filter.",
    )

    score_events = subparsers.add_parser(
        "score-events",
        help="Score canonical events using the rule-based ranking engine.",
    )
    score_events.add_argument(
        "--ticker",
        help="Optional single-ticker filter.",
    )

    serve_api = subparsers.add_parser(
        "serve-api",
        help="Run the local API for ranked events.",
    )
    serve_api.add_argument("--host", default="127.0.0.1")
    serve_api.add_argument("--port", type=int, default=8000)

    return parser


def cmd_bootstrap(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    db_path = args.db_path or paths.db_path
    db.init_database(db_path=db_path, schema_dir=paths.sql_dir)
    print(f"Bootstrap complete. DuckDB initialized at {db_path}")
    return 0


def cmd_ingest_sec_reference(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()

    payload = sec.fetch_json(sec.SEC_TICKER_REFERENCE_URL, user_agent=args.user_agent)
    references = sec.parse_company_tickers(payload)
    raw_path, bronze_path = sec.persist_reference_snapshot(paths, payload, references)

    if not args.skip_db:
        db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
        db.upsert_ticker_references(paths.db_path, references)
        db.record_ingestion_run(
            db_path=paths.db_path,
            pipeline_name="sec_ticker_reference",
            status="success",
            row_count=len(references),
            metadata={"raw_path": str(raw_path), "bronze_path": str(bronze_path)},
        )

    print(f"Stored {len(references)} ticker references")
    print(f"Raw snapshot: {raw_path}")
    print(f"Bronze snapshot: {bronze_path}")
    return 0


def cmd_ingest_sec_filings(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()

    reference_map = sec.load_or_fetch_reference_map(
        paths=paths,
        user_agent=args.user_agent,
        refresh=args.refresh_reference,
    )

    requested_tickers = [ticker.upper() for ticker in args.tickers]
    unknown_tickers = [ticker for ticker in requested_tickers if ticker not in reference_map]
    if unknown_tickers:
        raise SystemExit(f"Unknown ticker(s) in SEC reference map: {', '.join(unknown_tickers)}")

    filtered_forms = {form.upper() for form in args.forms}
    filings = []
    raw_paths: dict[str, str] = {}

    for ticker in requested_tickers:
        reference = reference_map[ticker]
        payload = sec.fetch_json(
            sec.build_submission_url(reference.cik),
            user_agent=args.user_agent,
        )
        raw_path = sec.persist_submission_snapshot(paths, reference.cik, payload)
        raw_paths[ticker] = str(raw_path)

        ticker_filings = sec.parse_recent_filings(
            payload=payload,
            ticker=ticker,
            raw_path=raw_path,
        )
        if filtered_forms:
            ticker_filings = [
                filing for filing in ticker_filings if filing.form_type.upper() in filtered_forms
            ]
        filings.extend(ticker_filings[: args.per_ticker_limit])

    bronze_path = sec.persist_filing_snapshot(paths, filings)

    if not args.skip_db:
        db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
        db.upsert_raw_filings(paths.db_path, filings)
        db.record_ingestion_run(
            db_path=paths.db_path,
            pipeline_name="sec_filings",
            status="success",
            row_count=len(filings),
            metadata={
                "tickers": requested_tickers,
                "forms": sorted(filtered_forms),
                "bronze_path": str(bronze_path),
                "raw_paths": raw_paths,
            },
        )

    print(f"Stored {len(filings)} filing rows")
    print(f"Bronze snapshot: {bronze_path}")
    return 0


def cmd_ingest_market_daily(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    bars = daily_features.load_market_bars_from_csv(args.csv, source=args.source)
    db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
    inserted = db.upsert_market_bars_daily(paths.db_path, bars)
    db.record_ingestion_run(
        db_path=paths.db_path,
        pipeline_name="market_bars_daily_csv",
        status="success",
        row_count=inserted,
        metadata={"csv_path": str(args.csv), "source": args.source},
    )
    print(f"Stored {inserted} daily market bars from {args.csv}")
    return 0


def cmd_build_sec_events(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
    filings = db.load_raw_filings(paths.db_path, forms=args.forms)
    events = sec_events.build_canonical_events_from_filings(
        filings,
        sentiment_backend_name=args.sentiment_backend,
        novelty_backend_name=args.novelty_backend,
        sentiment_model=args.sentiment_model,
        novelty_model=args.novelty_model,
    )
    inserted = db.upsert_events(paths.db_path, events)
    db.record_ingestion_run(
        db_path=paths.db_path,
        pipeline_name="build_sec_events",
        status="success",
        row_count=inserted,
        metadata={
            "forms": args.forms,
            "sentiment_backend": args.sentiment_backend,
            "sentiment_model": args.sentiment_model,
            "novelty_backend": args.novelty_backend,
            "novelty_model": args.novelty_model,
        },
    )
    print(f"Built {inserted} canonical events")
    return 0


def cmd_compute_daily_features(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
    events = db.load_events(paths.db_path, ticker=args.ticker)
    bars = db.load_market_bars_daily(paths.db_path, ticker=args.ticker)
    features = daily_features.compute_event_market_features(events, bars)
    inserted = db.upsert_event_market_features(paths.db_path, features)
    db.record_ingestion_run(
        db_path=paths.db_path,
        pipeline_name="compute_daily_features",
        status="success",
        row_count=inserted,
        metadata={"ticker": args.ticker},
    )
    print(f"Computed {inserted} daily feature rows")
    return 0


def cmd_score_events(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
    events = db.load_events(paths.db_path, ticker=args.ticker)
    scores = rules.score_events_from_database(paths.db_path, events)
    inserted = db.upsert_event_scores(paths.db_path, scores)
    db.record_ingestion_run(
        db_path=paths.db_path,
        pipeline_name="score_events",
        status="success",
        row_count=inserted,
        metadata={"ticker": args.ticker},
    )
    print(f"Scored {inserted} events")
    return 0


def cmd_serve_api(args: argparse.Namespace) -> int:
    try:
        import uvicorn
    except ImportError as exc:
        raise RuntimeError(
            "uvicorn is required for `serve-api`. Install dependencies with `pip install -e .`."
        ) from exc

    uvicorn.run(
        "pre_news_trading_surveillance.api.app:app",
        host=args.host,
        port=args.port,
        reload=False,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "bootstrap":
        return cmd_bootstrap(args)
    if args.command == "ingest-sec-reference":
        return cmd_ingest_sec_reference(args)
    if args.command == "ingest-sec-filings":
        return cmd_ingest_sec_filings(args)
    if args.command == "ingest-market-daily":
        return cmd_ingest_market_daily(args)
    if args.command == "build-sec-events":
        return cmd_build_sec_events(args)
    if args.command == "compute-daily-features":
        return cmd_compute_daily_features(args)
    if args.command == "score-events":
        return cmd_score_events(args)
    if args.command == "serve-api":
        return cmd_serve_api(args)

    parser.error(f"Unknown command: {args.command}")
    return 2
