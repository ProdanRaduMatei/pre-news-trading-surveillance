from __future__ import annotations

import argparse
from pathlib import Path

from . import db
from .ingest import sec
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

    return parser


def cmd_bootstrap(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    db_path = args.db_path or paths.db_path
    db.init_database(db_path=db_path, schema_path=paths.sql_dir / "001_init.sql")
    print(f"Bootstrap complete. DuckDB initialized at {db_path}")
    return 0


def cmd_ingest_sec_reference(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()

    payload = sec.fetch_json(sec.SEC_TICKER_REFERENCE_URL, user_agent=args.user_agent)
    references = sec.parse_company_tickers(payload)
    raw_path, bronze_path = sec.persist_reference_snapshot(paths, payload, references)

    if not args.skip_db:
        db.init_database(db_path=paths.db_path, schema_path=paths.sql_dir / "001_init.sql")
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
        db.init_database(db_path=paths.db_path, schema_path=paths.sql_dir / "001_init.sql")
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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "bootstrap":
        return cmd_bootstrap(args)
    if args.command == "ingest-sec-reference":
        return cmd_ingest_sec_reference(args)
    if args.command == "ingest-sec-filings":
        return cmd_ingest_sec_filings(args)

    parser.error(f"Unknown command: {args.command}")
    return 2
