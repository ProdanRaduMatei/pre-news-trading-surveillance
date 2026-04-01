from __future__ import annotations

import sys
import tempfile
import unittest
from argparse import Namespace
from dataclasses import replace
from pathlib import Path
from urllib.error import HTTPError
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pre_news_trading_surveillance import db  # noqa: E402
from pre_news_trading_surveillance.cli import (  # noqa: E402
    cmd_build_sec_events,
    cmd_compute_daily_features,
    cmd_ingest_market_minute,
    cmd_score_events,
)
from pre_news_trading_surveillance.domain import MarketBarDaily  # noqa: E402
from pre_news_trading_surveillance.ingest import sec  # noqa: E402
from pre_news_trading_surveillance.ingest.models import RawFilingRecord  # noqa: E402
from pre_news_trading_surveillance.settings import default_paths  # noqa: E402


class IngestionObservabilityTests(unittest.TestCase):
    def test_begin_and_finish_ingestion_run_records_artifacts_and_duration(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = replace(default_paths(root=root), sql_dir=Path(__file__).resolve().parents[1] / "sql")
            paths.ensure_directories()
            db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)

            run_id = db.begin_ingestion_run(
                db_path=paths.db_path,
                pipeline_name="unit_test_pipeline",
                metadata={"phase": "start"},
                parent_run_id="parent-run",
            )
            db.finish_ingestion_run(
                db_path=paths.db_path,
                run_id=run_id,
                status="success",
                row_count=7,
                metadata={"phase": "finish"},
                artifact_paths=["/tmp/artifact.ndjson"],
                attempt_count=3,
            )

            runs = db.list_ingestion_runs(paths.db_path, limit=5, pipeline_name="unit_test_pipeline")

        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["run_id"], run_id)
        self.assertEqual(runs[0]["status"], "success")
        self.assertEqual(runs[0]["row_count"], 7)
        self.assertEqual(runs[0]["metadata"], {"phase": "finish"})
        self.assertEqual(runs[0]["artifact_paths"], ["/tmp/artifact.ndjson"])
        self.assertEqual(runs[0]["parent_run_id"], "parent-run")
        self.assertEqual(runs[0]["attempt_count"], 3)
        self.assertIsNotNone(runs[0]["started_at"])
        self.assertIsNotNone(runs[0]["finished_at"])

    def test_sec_fetch_json_retries_and_marks_rate_limit(self) -> None:
        class Response:
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"ok": true}'

        metrics: dict[str, object] = {}
        with patch(
            "pre_news_trading_surveillance.ingest.sec.urlopen",
            side_effect=[
                HTTPError(
                    sec.SEC_TICKER_REFERENCE_URL,
                    429,
                    "Too Many Requests",
                    hdrs=None,
                    fp=None,
                ),
                Response(),
            ],
        ):
            payload = sec.fetch_json(
                sec.SEC_TICKER_REFERENCE_URL,
                user_agent="Unit Test test@example.com",
                retry_attempts=2,
                retry_backoff_seconds=0,
                metrics=metrics,
            )

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(metrics["attempt_count"], 2)
        self.assertTrue(metrics["rate_limited"])

    def test_ingest_market_minute_csv_copies_raw_snapshot_and_records_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = replace(default_paths(root=root), sql_dir=Path(__file__).resolve().parents[1] / "sql")
            paths.ensure_directories()
            csv_path = root / "minute.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "ticker,timestamp,open,high,low,close,volume",
                        "AAPL,2024-01-15T13:30:00+00:00,100,101,99.5,100.5,1500",
                        "AAPL,2024-01-15T13:31:00+00:00,100.5,101.2,100.2,101.0,1700",
                    ]
                ),
                encoding="utf-8",
            )

            with patch("pre_news_trading_surveillance.cli.default_paths", return_value=paths):
                exit_code = cmd_ingest_market_minute(
                    Namespace(csv=csv_path, source="local_csv", parent_run_id=None)
                )

            self.assertEqual(exit_code, 0)
            snapshots = list((paths.raw_dir / "market" / "csv" / "minute").glob("minute_*.csv"))
            self.assertEqual(len(snapshots), 1)
            self.assertIn("ticker,timestamp,open,high,low,close,volume", snapshots[0].read_text(encoding="utf-8"))

            run = db.list_ingestion_runs(
                paths.db_path,
                limit=1,
                pipeline_name="market_bars_minute_csv",
            )[0]

        self.assertEqual(run["status"], "success")
        self.assertEqual(run["row_count"], 2)
        self.assertEqual(run["metadata"]["raw_snapshot_path"], str(snapshots[0]))
        self.assertIn(str(snapshots[0]), run["artifact_paths"])

    def test_score_events_persists_input_output_snapshots_and_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = replace(default_paths(root=root), sql_dir=Path(__file__).resolve().parents[1] / "sql")
            paths.ensure_directories()
            db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)

            filing = RawFilingRecord(
                filing_id="0001:score-lineage",
                ticker="AAPL",
                cik="0000320193",
                company_name="Apple Inc.",
                accession_no="0000320193-24-000010",
                form_type="8-K",
                filing_date="2024-01-15",
                accepted_at="2024-01-15T13:30:00+00:00",
                items_json='["2.01", "9.01"]',
                primary_document="deal.htm",
                primary_doc_description="Completion of Acquisition",
                source_url="https://example.com/deal.htm",
                raw_path="/tmp/raw.json",
                ingested_at="2024-01-15T13:31:00+00:00",
            )
            db.upsert_raw_filings(paths.db_path, [filing])
            db.record_ingestion_run(
                db_path=paths.db_path,
                pipeline_name="sec_filings",
                status="success",
                row_count=1,
                metadata={"tickers": ["AAPL"]},
                artifact_paths=["/tmp/sec_filings.ndjson"],
            )

            bars = [
                MarketBarDaily(
                    bar_id=f"AAPL:2024-01-{day:02d}",
                    ticker="AAPL",
                    trading_date=f"2024-01-{day:02d}",
                    open=100 + day,
                    high=101 + day,
                    low=99 + day,
                    close=100 + day,
                    volume=1_000_000 + day * 10_000,
                    source="test",
                    ingested_at="2024-01-20T00:00:00+00:00",
                )
                for day in range(2, 15)
            ]
            db.upsert_market_bars_daily(paths.db_path, bars)
            db.record_ingestion_run(
                db_path=paths.db_path,
                pipeline_name="market_bars_daily_csv",
                status="success",
                row_count=len(bars),
                metadata={"source": "test"},
                artifact_paths=["/tmp/market_daily.csv"],
            )

            with patch("pre_news_trading_surveillance.cli.default_paths", return_value=paths):
                cmd_build_sec_events(
                    Namespace(
                        forms=["8-K"],
                        sentiment_backend="heuristic",
                        sentiment_model=None,
                        novelty_backend="lexical",
                        novelty_model=None,
                        parent_run_id=None,
                    )
                )
                cmd_compute_daily_features(Namespace(ticker=None, parent_run_id=None))
                cmd_score_events(Namespace(ticker=None, parent_run_id=None))

            score_run = db.list_ingestion_runs(
                paths.db_path,
                limit=1,
                pipeline_name="score_events",
            )[0]
            input_snapshots = list((paths.silver_dir / "scoring").glob("score_inputs_*.ndjson"))
            output_snapshots = list((paths.silver_dir / "scoring").glob("event_scores_*.ndjson"))

        self.assertEqual(score_run["status"], "success")
        self.assertEqual(score_run["row_count"], 1)
        self.assertEqual(len(input_snapshots), 1)
        self.assertEqual(len(output_snapshots), 1)
        self.assertIn(str(input_snapshots[0]), score_run["artifact_paths"])
        self.assertIn(str(output_snapshots[0]), score_run["artifact_paths"])
        self.assertIn("build_sec_events", score_run["metadata"]["upstream_runs"])
        self.assertIn("compute_daily_features", score_run["metadata"]["upstream_runs"])


if __name__ == "__main__":
    unittest.main()
