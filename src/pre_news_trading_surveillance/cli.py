from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from pathlib import Path

from . import artifacts, db
from .events import sec_events
from .features import daily as daily_features
from .features import minute as minute_features
from .ingest import issuer_releases, market, sec
from .pipeline import refresh as refresh_pipeline
from .publish import snapshot as publish_snapshot
from .publish import storage as publish_storage
from .scoring import anomaly_stack, rules
from .serve_policy import ServePolicy
from .settings import default_paths


@dataclass
class IngestionRunTracker:
    db_path: Path
    run_id: str
    pipeline_name: str
    metadata: dict[str, object] = field(default_factory=dict)
    artifact_paths: list[str] = field(default_factory=list)
    attempt_count: int = 0
    row_count: int = 0

    def add_artifact(self, path: Path | str | None) -> None:
        if path is None:
            return
        self.artifact_paths.append(str(path))

    def add_attempts(self, attempts: int | None) -> None:
        if attempts is None:
            return
        self.attempt_count += max(int(attempts), 0)


@contextmanager
def tracked_ingestion_run(
    *,
    paths,
    pipeline_name: str,
    metadata: dict[str, object] | None = None,
    parent_run_id: str | None = None,
):
    db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
    tracker = IngestionRunTracker(
        db_path=paths.db_path,
        run_id=db.begin_ingestion_run(
            db_path=paths.db_path,
            pipeline_name=pipeline_name,
            metadata=metadata or {},
            parent_run_id=parent_run_id,
        ),
        pipeline_name=pipeline_name,
        metadata=dict(metadata or {}),
    )
    try:
        yield tracker
    except Exception as exc:
        db.finish_ingestion_run(
            db_path=paths.db_path,
            run_id=tracker.run_id,
            status="failed",
            row_count=tracker.row_count,
            metadata=tracker.metadata,
            error_message=str(exc),
            artifact_paths=tracker.artifact_paths,
            attempt_count=tracker.attempt_count,
        )
        raise
    else:
        db.finish_ingestion_run(
            db_path=paths.db_path,
            run_id=tracker.run_id,
            status="success",
            row_count=tracker.row_count,
            metadata=tracker.metadata,
            artifact_paths=tracker.artifact_paths,
            attempt_count=tracker.attempt_count,
        )


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

    ingest_press_releases = subparsers.add_parser(
        "ingest-press-releases",
        help="Fetch official issuer press releases or earnings releases from configured RSS/Atom feeds.",
    )
    ingest_press_releases.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the issuer feed TOML config.",
    )
    ingest_press_releases.add_argument(
        "--tickers",
        nargs="*",
        help="Optional ticker filter for a subset of configured feeds.",
    )
    ingest_press_releases.add_argument(
        "--per-feed-limit",
        type=int,
        default=25,
        help="Maximum number of feed entries to retain per issuer feed.",
    )
    ingest_press_releases.add_argument(
        "--user-agent",
        default=market.DEFAULT_USER_AGENT,
        help="Optional user agent used for issuer feed requests.",
    )
    ingest_press_releases.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="Issuer feed request timeout in seconds.",
    )
    ingest_press_releases.add_argument(
        "--skip-db",
        action="store_true",
        help="Only write filesystem artifacts and skip database loading.",
    )

    ingest_market_daily = subparsers.add_parser(
        "ingest-market-daily",
        help="Load daily market bars from a CSV file or provider into storage.",
    )
    daily_source = ingest_market_daily.add_mutually_exclusive_group(required=True)
    daily_source.add_argument("--csv", type=Path, help="CSV path.")
    daily_source.add_argument(
        "--provider",
        choices=[market.ALPHA_VANTAGE_PROVIDER],
        help="Remote market data provider.",
    )
    ingest_market_daily.add_argument(
        "--tickers",
        nargs="+",
        help="Ticker symbols to fetch when using a provider.",
    )
    ingest_market_daily.add_argument(
        "--source",
        default="csv_import",
        help="Source label stored with the imported bars.",
    )
    ingest_market_daily.add_argument("--api-key", help="Explicit provider API key.")
    ingest_market_daily.add_argument(
        "--api-key-env",
        default=market.ALPHA_VANTAGE_ENV_VAR,
        help="Environment variable to read the provider API key from.",
    )
    ingest_market_daily.add_argument(
        "--outputsize",
        choices=["compact", "full"],
        default="compact",
        help="Provider output size.",
    )
    ingest_market_daily.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="Provider request timeout in seconds.",
    )

    ingest_market_minute = subparsers.add_parser(
        "ingest-market-minute",
        help="Load minute market bars from a CSV file or provider into storage.",
    )
    minute_source = ingest_market_minute.add_mutually_exclusive_group(required=True)
    minute_source.add_argument("--csv", type=Path, help="CSV path.")
    minute_source.add_argument(
        "--provider",
        choices=[market.ALPHA_VANTAGE_PROVIDER],
        help="Remote market data provider.",
    )
    ingest_market_minute.add_argument(
        "--tickers",
        nargs="+",
        help="Ticker symbols to fetch when using a provider.",
    )
    ingest_market_minute.add_argument(
        "--source",
        default="csv_import",
        help="Source label stored with the imported bars.",
    )
    ingest_market_minute.add_argument("--api-key", help="Explicit provider API key.")
    ingest_market_minute.add_argument(
        "--api-key-env",
        default=market.ALPHA_VANTAGE_ENV_VAR,
        help="Environment variable to read the provider API key from.",
    )
    ingest_market_minute.add_argument(
        "--interval",
        choices=["1min", "5min", "15min", "30min", "60min"],
        default="1min",
        help="Provider bar interval.",
    )
    ingest_market_minute.add_argument(
        "--outputsize",
        choices=["compact", "full"],
        default="compact",
        help="Provider output size.",
    )
    ingest_market_minute.add_argument(
        "--month",
        help="Optional provider month in YYYY-MM format for historical intraday pulls.",
    )
    ingest_market_minute.add_argument(
        "--entitlement",
        choices=["realtime", "delayed"],
        help="Optional provider entitlement mode for intraday data.",
    )
    ingest_market_minute.add_argument(
        "--adjusted",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Whether provider intraday bars should be split/dividend adjusted.",
    )
    ingest_market_minute.add_argument(
        "--extended-hours",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Whether provider intraday bars should include extended hours.",
    )
    ingest_market_minute.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="Provider request timeout in seconds.",
    )

    build_events = subparsers.add_parser(
        "build-sec-events",
        help="Build canonical official events from SEC filings and issuer press releases.",
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

    compute_minute_features = subparsers.add_parser(
        "compute-minute-features",
        help="Compute minute pre-event market features for canonical events.",
    )
    compute_minute_features.add_argument(
        "--ticker",
        help="Optional single-ticker filter.",
    )

    score_events = subparsers.add_parser(
        "score-events",
        help="Score canonical events using rules or the hybrid anomaly stack.",
    )
    score_events.add_argument(
        "--ticker",
        help="Optional single-ticker filter.",
    )
    score_events.add_argument(
        "--engine",
        choices=["auto", "rules", "hybrid"],
        default="auto",
        help="Scoring engine. `auto` uses trained models when available and falls back to rules otherwise.",
    )
    score_events.add_argument(
        "--model-dir",
        type=Path,
        help="Optional model directory override. Defaults to data/models/scoring/current.",
    )

    train_model_stack = subparsers.add_parser(
        "train-model-stack",
        help="Train the IsolationForest plus optional LightGBM ranker on engineered event features.",
    )
    train_model_stack.add_argument(
        "--ticker",
        help="Optional single-ticker filter.",
    )
    train_model_stack.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory for trained model artifacts. Defaults to data/models/scoring/current.",
    )
    train_model_stack.add_argument(
        "--contamination",
        type=float,
        default=0.12,
        help="IsolationForest contamination parameter.",
    )
    train_model_stack.add_argument(
        "--min-samples",
        type=int,
        default=12,
        help="Minimum events required before training will run.",
    )
    train_model_stack.add_argument(
        "--use-ranker",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Whether to train the LightGBM ranker on top of the anomaly features.",
    )

    publish_job = subparsers.add_parser(
        "publish-snapshot",
        help="Build a public JSON snapshot bundle from the scored DuckDB dataset.",
    )
    publish_job.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory for the published bundle. Defaults to data/publish/current.",
    )
    publish_job.add_argument(
        "--events-limit",
        type=int,
        default=250,
        help="Maximum number of ranked events to include in the published bundle.",
    )
    publish_job.add_argument(
        "--public-safe",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Whether to apply delayed public-safe filtering before writing the bundle.",
    )
    publish_job.add_argument(
        "--public-delay-minutes",
        type=int,
        default=1440,
        help="Visibility delay in minutes for the published bundle when public-safe mode is enabled.",
    )
    publish_job.add_argument(
        "--s3-bucket",
        help="Optional S3-compatible bucket to upload the bundle to.",
    )
    publish_job.add_argument(
        "--s3-prefix",
        default="current",
        help="Optional key prefix when uploading to S3-compatible storage.",
    )
    publish_job.add_argument(
        "--s3-region",
        help="Optional S3 region name.",
    )
    publish_job.add_argument(
        "--s3-endpoint-url",
        help="Optional custom endpoint URL for S3-compatible storage.",
    )
    publish_job.add_argument(
        "--s3-access-key-env",
        default="AWS_ACCESS_KEY_ID",
        help="Environment variable for S3 access key.",
    )
    publish_job.add_argument(
        "--s3-secret-key-env",
        default="AWS_SECRET_ACCESS_KEY",
        help="Environment variable for S3 secret key.",
    )
    publish_job.add_argument(
        "--s3-session-token-env",
        default="AWS_SESSION_TOKEN",
        help="Environment variable for S3 session token.",
    )

    refresh_job = subparsers.add_parser(
        "refresh-pipeline",
        help="Run the scheduled end-to-end refresh pipeline from a config file.",
    )
    refresh_job.add_argument(
        "--config",
        type=Path,
        default=Path("configs/refresh_pipeline.example.toml"),
        help="Path to the refresh pipeline TOML config.",
    )
    refresh_job.add_argument(
        "--mode",
        choices=["full", "intraday"],
        default="full",
        help="Predefined refresh mode when explicit steps are not provided.",
    )
    refresh_job.add_argument(
        "--steps",
        nargs="*",
        help="Optional explicit refresh steps to run.",
    )

    serve_api = subparsers.add_parser(
        "serve-api",
        help="Run the local API for ranked events.",
    )
    serve_api.add_argument("--host", default="127.0.0.1")
    serve_api.add_argument("--port", type=int, default=8000)

    list_runs = subparsers.add_parser(
        "list-ingestion-runs",
        help="List recent ingestion and scoring runs with status and artifact visibility.",
    )
    list_runs.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of runs to return.",
    )
    list_runs.add_argument(
        "--pipeline-name",
        help="Optional pipeline filter.",
    )
    list_runs.add_argument(
        "--status",
        help="Optional status filter such as running, success, or failed.",
    )

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

    run_context = (
        tracked_ingestion_run(
            paths=paths,
            pipeline_name="sec_ticker_reference",
            metadata={"source_url": sec.SEC_TICKER_REFERENCE_URL},
            parent_run_id=getattr(args, "parent_run_id", None),
        )
        if not args.skip_db
        else nullcontext(None)
    )
    with run_context as tracker:
        fetch_metrics: dict[str, object] = {}
        payload = sec.fetch_json(
            sec.SEC_TICKER_REFERENCE_URL,
            user_agent=args.user_agent,
            metrics=fetch_metrics,
        )
        references = sec.parse_company_tickers(payload)
        raw_path, bronze_path = sec.persist_reference_snapshot(paths, payload, references)

        if tracker is not None:
            db.upsert_ticker_references(paths.db_path, references)
            tracker.row_count = len(references)
            tracker.add_attempts(fetch_metrics.get("attempt_count"))
            tracker.add_artifact(raw_path)
            tracker.add_artifact(bronze_path)
            tracker.metadata.update(
                {
                    "fetch_metrics": fetch_metrics,
                    "raw_path": str(raw_path),
                    "bronze_path": str(bronze_path),
                }
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
    run_context = (
        tracked_ingestion_run(
            paths=paths,
            pipeline_name="sec_filings",
            metadata={
                "tickers": requested_tickers,
                "forms": sorted(filtered_forms),
                "per_ticker_limit": args.per_ticker_limit,
                "refresh_reference": args.refresh_reference,
            },
            parent_run_id=getattr(args, "parent_run_id", None),
        )
        if not args.skip_db
        else nullcontext(None)
    )
    with run_context as tracker:
        filings = []
        raw_paths: dict[str, str] = {}
        fetch_metrics_by_ticker: dict[str, dict[str, object]] = {}

        for ticker in requested_tickers:
            reference = reference_map[ticker]
            fetch_metrics: dict[str, object] = {}
            payload = sec.fetch_json(
                sec.build_submission_url(reference.cik),
                user_agent=args.user_agent,
                metrics=fetch_metrics,
            )
            raw_path = sec.persist_submission_snapshot(paths, reference.cik, payload)
            raw_paths[ticker] = str(raw_path)
            fetch_metrics_by_ticker[ticker] = fetch_metrics

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

            if tracker is not None:
                tracker.add_artifact(raw_path)
                tracker.add_attempts(fetch_metrics.get("attempt_count"))

        bronze_path = sec.persist_filing_snapshot(paths, filings)

        if tracker is not None:
            db.upsert_raw_filings(paths.db_path, filings)
            tracker.row_count = len(filings)
            tracker.add_artifact(bronze_path)
            tracker.metadata.update(
                {
                    "bronze_path": str(bronze_path),
                    "raw_paths": raw_paths,
                    "fetch_metrics_by_ticker": fetch_metrics_by_ticker,
                }
            )

    print(f"Stored {len(filings)} filing rows")
    print(f"Bronze snapshot: {bronze_path}")
    return 0


def cmd_ingest_press_releases(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    config_path = args.config
    if not config_path.is_absolute():
        config_path = paths.root / config_path
    if not config_path.exists():
        raise SystemExit(f"Issuer feed config not found: {config_path}")

    feeds = issuer_releases.load_feed_configs(config_path, tickers=args.tickers)
    run_context = (
        tracked_ingestion_run(
            paths=paths,
            pipeline_name="issuer_press_releases",
            metadata={
                "config_path": str(config_path),
                "tickers": [feed.ticker for feed in feeds],
                "per_feed_limit": args.per_feed_limit,
                "user_agent": args.user_agent,
            },
            parent_run_id=getattr(args, "parent_run_id", None),
        )
        if not args.skip_db
        else nullcontext(None)
    )
    with run_context as tracker:
        releases = []
        raw_paths: dict[str, str] = {}
        fetch_metrics_by_ticker: dict[str, dict[str, object]] = {}
        for feed in feeds:
            fetch_metrics: dict[str, object] = {}
            xml_text = issuer_releases.fetch_feed_xml(
                feed.feed_url,
                user_agent=args.user_agent,
                timeout_seconds=args.timeout_seconds,
                metrics=fetch_metrics,
            )
            raw_path = issuer_releases.persist_feed_snapshot(paths, feed=feed, xml_text=xml_text)
            raw_paths[feed.ticker] = str(raw_path)
            fetch_metrics_by_ticker[feed.ticker] = fetch_metrics
            ticker_releases = issuer_releases.parse_feed_releases(
                xml_text,
                feed=feed,
                raw_path=raw_path,
                per_feed_limit=args.per_feed_limit,
            )
            releases.extend(ticker_releases)
            if tracker is not None:
                tracker.add_artifact(raw_path)
                tracker.add_attempts(fetch_metrics.get("attempt_count"))

        bronze_path = issuer_releases.persist_release_snapshot(paths, releases)
        if tracker is not None:
            db.upsert_raw_issuer_releases(paths.db_path, releases)
            tracker.row_count = len(releases)
            tracker.add_artifact(bronze_path)
            tracker.metadata.update(
                {
                    "bronze_path": str(bronze_path),
                    "raw_paths": raw_paths,
                    "fetch_metrics_by_ticker": fetch_metrics_by_ticker,
                }
            )

    print(f"Stored {len(releases)} issuer release rows")
    print(f"Bronze snapshot: {bronze_path}")
    return 0


def cmd_ingest_market_daily(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    if args.csv:
        with tracked_ingestion_run(
            paths=paths,
            pipeline_name="market_bars_daily_csv",
            metadata={"source": args.source, "csv_path": str(args.csv)},
            parent_run_id=getattr(args, "parent_run_id", None),
        ) as tracker:
            bars = daily_features.load_market_bars_from_csv(args.csv, source=args.source)
            raw_snapshot_path = market.persist_local_market_snapshot(
                paths,
                granularity="daily",
                csv_path=args.csv,
            )
            inserted = db.upsert_market_bars_daily(paths.db_path, bars)
            tracker.row_count = inserted
            tracker.add_artifact(raw_snapshot_path)
            tracker.metadata.update({"raw_snapshot_path": str(raw_snapshot_path)})
        print(f"Stored {inserted} daily market bars from {args.csv}")
        return 0

    requested_tickers = _require_provider_tickers(args.tickers)
    with tracked_ingestion_run(
        paths=paths,
        pipeline_name="market_bars_daily_provider",
        metadata={
            "provider": args.provider,
            "tickers": requested_tickers,
            "outputsize": args.outputsize,
            "timeout_seconds": args.timeout_seconds,
        },
        parent_run_id=getattr(args, "parent_run_id", None),
    ) as tracker:
        api_key = market.resolve_api_key(args.api_key, args.api_key_env)
        bars = []
        raw_paths: dict[str, str] = {}
        fetch_metrics_by_ticker: dict[str, dict[str, object]] = {}
        for ticker in requested_tickers:
            fetch_metrics: dict[str, object] = {}
            text = market.fetch_alpha_vantage_daily_csv(
                ticker,
                api_key,
                outputsize=args.outputsize,
                timeout_seconds=args.timeout_seconds,
                metrics=fetch_metrics,
            )
            raw_path = market.persist_raw_market_snapshot(
                paths,
                provider=args.provider,
                granularity="daily",
                ticker=ticker,
                text=text,
                descriptor=f"daily_{args.outputsize}",
            )
            raw_paths[ticker] = str(raw_path)
            fetch_metrics_by_ticker[ticker] = fetch_metrics
            tracker.add_artifact(raw_path)
            tracker.add_attempts(fetch_metrics.get("attempt_count"))
            bars.extend(
                market.parse_alpha_vantage_daily_csv(
                    text,
                    symbol=ticker,
                    source=f"{args.provider}_api",
                )
            )

        inserted = db.upsert_market_bars_daily(paths.db_path, bars)
        tracker.row_count = inserted
        tracker.metadata.update(
            {
                "raw_paths": raw_paths,
                "fetch_metrics_by_ticker": fetch_metrics_by_ticker,
            }
        )
    print(f"Stored {inserted} daily market bars from provider {args.provider}")
    return 0


def cmd_ingest_market_minute(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    if args.csv:
        with tracked_ingestion_run(
            paths=paths,
            pipeline_name="market_bars_minute_csv",
            metadata={"source": args.source, "csv_path": str(args.csv)},
            parent_run_id=getattr(args, "parent_run_id", None),
        ) as tracker:
            bars = minute_features.load_market_bars_from_csv(args.csv, source=args.source)
            raw_snapshot_path = market.persist_local_market_snapshot(
                paths,
                granularity="minute",
                csv_path=args.csv,
            )
            inserted = db.upsert_market_bars_minute(paths.db_path, bars)
            tracker.row_count = inserted
            tracker.add_artifact(raw_snapshot_path)
            tracker.metadata.update({"raw_snapshot_path": str(raw_snapshot_path)})
        print(f"Stored {inserted} minute market bars from {args.csv}")
        return 0

    requested_tickers = _require_provider_tickers(args.tickers)
    with tracked_ingestion_run(
        paths=paths,
        pipeline_name="market_bars_minute_provider",
        metadata={
            "provider": args.provider,
            "tickers": requested_tickers,
            "interval": args.interval,
            "outputsize": args.outputsize,
            "month": args.month,
            "entitlement": args.entitlement,
            "adjusted": args.adjusted,
            "extended_hours": args.extended_hours,
            "timeout_seconds": args.timeout_seconds,
        },
        parent_run_id=getattr(args, "parent_run_id", None),
    ) as tracker:
        api_key = market.resolve_api_key(args.api_key, args.api_key_env)
        bars = []
        raw_paths: dict[str, str] = {}
        fetch_metrics_by_ticker: dict[str, dict[str, object]] = {}
        for ticker in requested_tickers:
            fetch_metrics: dict[str, object] = {}
            text = market.fetch_alpha_vantage_intraday_csv(
                ticker,
                api_key,
                interval=args.interval,
                adjusted=args.adjusted,
                extended_hours=args.extended_hours,
                outputsize=args.outputsize,
                month=args.month,
                entitlement=args.entitlement,
                timeout_seconds=args.timeout_seconds,
                metrics=fetch_metrics,
            )
            descriptor_parts = [f"intraday_{args.interval}", args.outputsize]
            if args.month:
                descriptor_parts.append(args.month)
            if args.entitlement:
                descriptor_parts.append(args.entitlement)
            raw_path = market.persist_raw_market_snapshot(
                paths,
                provider=args.provider,
                granularity="minute",
                ticker=ticker,
                text=text,
                descriptor="_".join(descriptor_parts),
            )
            raw_paths[ticker] = str(raw_path)
            fetch_metrics_by_ticker[ticker] = fetch_metrics
            tracker.add_artifact(raw_path)
            tracker.add_attempts(fetch_metrics.get("attempt_count"))
            bars.extend(
                market.parse_alpha_vantage_intraday_csv(
                    text,
                    symbol=ticker,
                    source=f"{args.provider}_api",
                )
            )

        inserted = db.upsert_market_bars_minute(paths.db_path, bars)
        tracker.row_count = inserted
        tracker.metadata.update(
            {
                "raw_paths": raw_paths,
                "fetch_metrics_by_ticker": fetch_metrics_by_ticker,
            }
        )
    print(f"Stored {inserted} minute market bars from provider {args.provider}")
    return 0


def cmd_build_sec_events(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    with tracked_ingestion_run(
        paths=paths,
        pipeline_name="build_sec_events",
        metadata={
            "forms": args.forms,
            "sentiment_backend": args.sentiment_backend,
            "sentiment_model": args.sentiment_model,
            "novelty_backend": args.novelty_backend,
            "novelty_model": args.novelty_model,
        },
        parent_run_id=getattr(args, "parent_run_id", None),
    ) as tracker:
        filings = db.load_raw_filings(paths.db_path, forms=args.forms)
        press_releases = db.load_raw_issuer_releases(paths.db_path)
        events = sec_events.build_canonical_events_from_sources(
            filings=filings,
            issuer_releases=press_releases,
            sentiment_backend_name=args.sentiment_backend,
            novelty_backend_name=args.novelty_backend,
            sentiment_model=args.sentiment_model,
            novelty_model=args.novelty_model,
        )
        snapshot_path = artifacts.write_ndjson_snapshot(
            paths.silver_dir / "events",
            name_prefix="official_events",
            rows=[event.as_dict() for event in events],
        )
        inserted = db.upsert_events(paths.db_path, events)
        tracker.row_count = inserted
        tracker.add_artifact(snapshot_path)
        tracker.metadata.update(
            {
                "input_filings": len(filings),
                "input_issuer_releases": len(press_releases),
                "snapshot_path": str(snapshot_path),
                "upstream_runs": _lineage_snapshot(
                    paths.db_path,
                    ["sec_filings", "sec_ticker_reference", "issuer_press_releases"],
                ),
            }
        )
    print(f"Built {inserted} canonical events")
    return 0


def cmd_compute_daily_features(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    with tracked_ingestion_run(
        paths=paths,
        pipeline_name="compute_daily_features",
        metadata={"ticker": args.ticker},
        parent_run_id=getattr(args, "parent_run_id", None),
    ) as tracker:
        events = db.load_events(paths.db_path, ticker=args.ticker)
        bars = db.load_market_bars_daily(paths.db_path, ticker=args.ticker)
        features = daily_features.compute_event_market_features(events, bars)
        snapshot_path = artifacts.write_ndjson_snapshot(
            paths.silver_dir / "features" / "daily",
            name_prefix="daily_event_features",
            rows=[feature.as_dict() for feature in features],
        )
        inserted = db.upsert_event_market_features(paths.db_path, features)
        tracker.row_count = inserted
        tracker.add_artifact(snapshot_path)
        tracker.metadata.update(
            {
                "events_loaded": len(events),
                "bars_loaded": len(bars),
                "snapshot_path": str(snapshot_path),
                "upstream_runs": _lineage_snapshot(
                    paths.db_path,
                    ["build_sec_events", "market_bars_daily_provider", "market_bars_daily_csv"],
                ),
            }
        )
    print(f"Computed {inserted} daily feature rows")
    return 0


def cmd_compute_minute_features(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    with tracked_ingestion_run(
        paths=paths,
        pipeline_name="compute_minute_features",
        metadata={"ticker": args.ticker},
        parent_run_id=getattr(args, "parent_run_id", None),
    ) as tracker:
        events = db.load_events(paths.db_path, ticker=args.ticker)
        bars = db.load_market_bars_minute(paths.db_path, ticker=args.ticker)
        features = minute_features.compute_event_market_features(events, bars)
        snapshot_path = artifacts.write_ndjson_snapshot(
            paths.silver_dir / "features" / "minute",
            name_prefix="minute_event_features",
            rows=[feature.as_dict() for feature in features],
        )
        inserted = db.upsert_event_market_features_minute(paths.db_path, features)
        tracker.row_count = inserted
        tracker.add_artifact(snapshot_path)
        tracker.metadata.update(
            {
                "events_loaded": len(events),
                "bars_loaded": len(bars),
                "snapshot_path": str(snapshot_path),
                "upstream_runs": _lineage_snapshot(
                    paths.db_path,
                    ["build_sec_events", "market_bars_minute_provider", "market_bars_minute_csv"],
                ),
            }
        )
    print(f"Computed {inserted} minute feature rows")
    return 0


def cmd_score_events(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    engine = getattr(args, "engine", "auto")
    model_dir = getattr(args, "model_dir", None) or (paths.models_dir / "scoring" / "current")
    with tracked_ingestion_run(
        paths=paths,
        pipeline_name="score_events",
        metadata={
            "ticker": args.ticker,
            "engine": engine,
            "model_dir": str(model_dir),
        },
        parent_run_id=getattr(args, "parent_run_id", None),
    ) as tracker:
        details = db.load_scoring_event_details(paths.db_path, ticker=args.ticker)
        inputs_snapshot_path = artifacts.write_ndjson_snapshot(
            paths.silver_dir / "scoring",
            name_prefix="score_inputs",
            rows=details,
        )
        scores, scoring_metadata = anomaly_stack.score_event_details(
            details,
            engine=engine,
            model_dir=model_dir,
        )
        outputs_snapshot_path = artifacts.write_ndjson_snapshot(
            paths.silver_dir / "scoring",
            name_prefix="event_scores",
            rows=[score.as_dict() for score in scores],
        )
        inserted = db.upsert_event_scores(paths.db_path, scores)
        tracker.row_count = inserted
        tracker.add_artifact(inputs_snapshot_path)
        tracker.add_artifact(outputs_snapshot_path)
        tracker.metadata.update(
            {
                "events_loaded": len(details),
                "score_inputs": len(details),
                "inputs_snapshot_path": str(inputs_snapshot_path),
                "outputs_snapshot_path": str(outputs_snapshot_path),
                "scoring_metadata": scoring_metadata,
                "upstream_runs": _lineage_snapshot(
                    paths.db_path,
                    [
                        "build_sec_events",
                        "compute_daily_features",
                        "compute_minute_features",
                    ],
                ),
            }
        )
    print(f"Scored {inserted} events")
    return 0


def cmd_train_model_stack(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    output_dir = args.output_dir or (paths.models_dir / "scoring" / "current")
    with tracked_ingestion_run(
        paths=paths,
        pipeline_name="train_model_stack",
        metadata={
            "ticker": args.ticker,
            "output_dir": str(output_dir),
            "contamination": args.contamination,
            "min_samples": args.min_samples,
            "use_ranker": args.use_ranker,
        },
        parent_run_id=getattr(args, "parent_run_id", None),
    ) as tracker:
        details = db.load_scoring_event_details(paths.db_path, ticker=args.ticker)
        inputs_snapshot_path = artifacts.write_ndjson_snapshot(
            paths.silver_dir / "scoring",
            name_prefix="training_inputs",
            rows=details,
        )
        trained = anomaly_stack.train_model_stack(
            details,
            output_dir=output_dir,
            contamination=args.contamination,
            min_samples=args.min_samples,
            enable_ranker=args.use_ranker,
        )
        tracker.row_count = len(details)
        tracker.add_artifact(inputs_snapshot_path)
        tracker.add_artifact(trained.bundle_path)
        tracker.add_artifact(trained.manifest_path)
        tracker.metadata.update(
            {
                "training_samples": len(details),
                "inputs_snapshot_path": str(inputs_snapshot_path),
                "model_manifest": trained.manifest,
                "upstream_runs": _lineage_snapshot(
                    paths.db_path,
                    ["compute_daily_features", "compute_minute_features", "build_sec_events"],
                ),
            }
        )
    print(f"Trained anomaly stack on {len(details)} events")
    print(f"Model bundle: {trained.bundle_path}")
    print(f"Manifest: {trained.manifest_path}")
    return 0


def cmd_publish_snapshot(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    with tracked_ingestion_run(
        paths=paths,
        pipeline_name="publish_snapshot",
        metadata={
            "events_limit": args.events_limit,
            "public_safe_mode": args.public_safe,
            "public_delay_minutes": args.public_delay_minutes,
        },
        parent_run_id=getattr(args, "parent_run_id", None),
    ) as tracker:
        output_dir = args.output_dir or (paths.publish_dir / "current")
        policy = ServePolicy(
            public_safe_mode=bool(args.public_safe),
            delay_minutes=max(int(args.public_delay_minutes), 0),
            data_source_mode="published",
        )
        bundle = publish_snapshot.build_snapshot_bundle(
            db_path=paths.db_path,
            events_limit=args.events_limit,
            policy=policy,
        )
        publish_snapshot.write_snapshot_bundle(bundle, output_dir)
        tracker.row_count = len(bundle.events)
        tracker.add_artifact(output_dir / "manifest.json")
        tracker.add_artifact(output_dir / "summary.json")
        tracker.add_artifact(output_dir / "events.json")
        tracker.metadata.update(
            {
                "output_dir": str(output_dir),
                "manifest_generated_at": bundle.manifest["generated_at"],
                "policy": bundle.manifest.get("policy", {}),
                "upstream_runs": _lineage_snapshot(paths.db_path, ["score_events"]),
            }
        )
        print(f"Published snapshot bundle with {len(bundle.events)} events to {output_dir}")

        if args.s3_bucket:
            uploaded = publish_storage.upload_directory_to_s3(
                source_dir=output_dir,
                bucket=args.s3_bucket,
                prefix=args.s3_prefix,
                region=args.s3_region,
                endpoint_url=args.s3_endpoint_url,
                access_key=publish_storage.resolve_optional_env(args.s3_access_key_env),
                secret_key=publish_storage.resolve_optional_env(args.s3_secret_key_env),
                session_token=publish_storage.resolve_optional_env(args.s3_session_token_env),
            )
            tracker.metadata.update(
                {
                    "s3_bucket": args.s3_bucket,
                    "s3_prefix": args.s3_prefix,
                    "uploaded_keys": uploaded[:50],
                }
            )
            print(f"Uploaded {len(uploaded)} files to s3://{args.s3_bucket}/{args.s3_prefix}".rstrip("/"))
    return 0


def cmd_refresh_pipeline(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    config_path = args.config
    if not config_path.is_absolute():
        config_path = paths.root / config_path
    if not config_path.exists():
        raise SystemExit(f"Refresh config not found: {config_path}")

    config = refresh_pipeline.load_refresh_config(config_path)
    steps = refresh_pipeline.resolve_refresh_steps(args.mode, args.steps)
    completed = refresh_pipeline.run_refresh_pipeline(
        config=config,
        steps=steps,
        cli_module=__import__(__name__, fromlist=["dummy"]),
        paths=paths,
    )
    print(f"Completed refresh pipeline with steps: {', '.join(completed)}")
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


def cmd_list_ingestion_runs(args: argparse.Namespace) -> int:
    paths = default_paths()
    paths.ensure_directories()
    db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
    runs = db.list_ingestion_runs(
        paths.db_path,
        limit=args.limit,
        pipeline_name=args.pipeline_name,
        status=args.status,
    )
    for run in runs:
        artifacts_count = len(run.get("artifact_paths", []))
        print(
            f"{run['started_at']} {run['status']} {run['pipeline_name']} "
            f"rows={run['row_count']} attempts={run.get('attempt_count', 0)} "
            f"artifacts={artifacts_count} run_id={run['run_id']}"
        )
    print(f"Returned {len(runs)} run(s)")
    return 0


def _require_provider_tickers(tickers: list[str] | None) -> list[str]:
    if not tickers:
        raise SystemExit("`--tickers` is required when using `--provider`.")
    return [ticker.upper() for ticker in tickers]


def _lineage_snapshot(db_path: Path, pipeline_names: list[str]) -> dict[str, dict[str, object]]:
    upstream_runs = db.get_latest_successful_runs(db_path, pipeline_names)
    lineage: dict[str, dict[str, object]] = {}
    for pipeline_name, run in upstream_runs.items():
        lineage[pipeline_name] = {
            "run_id": run["run_id"],
            "started_at": run["started_at"],
            "finished_at": run.get("finished_at"),
            "artifact_paths": run.get("artifact_paths", []),
            "attempt_count": run.get("attempt_count", 0),
        }
    return lineage


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "bootstrap":
        return cmd_bootstrap(args)
    if args.command == "ingest-sec-reference":
        return cmd_ingest_sec_reference(args)
    if args.command == "ingest-sec-filings":
        return cmd_ingest_sec_filings(args)
    if args.command == "ingest-press-releases":
        return cmd_ingest_press_releases(args)
    if args.command == "ingest-market-daily":
        return cmd_ingest_market_daily(args)
    if args.command == "ingest-market-minute":
        return cmd_ingest_market_minute(args)
    if args.command == "build-sec-events":
        return cmd_build_sec_events(args)
    if args.command == "compute-daily-features":
        return cmd_compute_daily_features(args)
    if args.command == "compute-minute-features":
        return cmd_compute_minute_features(args)
    if args.command == "train-model-stack":
        return cmd_train_model_stack(args)
    if args.command == "score-events":
        return cmd_score_events(args)
    if args.command == "publish-snapshot":
        return cmd_publish_snapshot(args)
    if args.command == "refresh-pipeline":
        return cmd_refresh_pipeline(args)
    if args.command == "serve-api":
        return cmd_serve_api(args)
    if args.command == "list-ingestion-runs":
        return cmd_list_ingestion_runs(args)

    parser.error(f"Unknown command: {args.command}")
    return 2
