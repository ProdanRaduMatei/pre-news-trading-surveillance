from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pre_news_trading_surveillance import db  # noqa: E402
from pre_news_trading_surveillance.events.sec_events import build_canonical_events_from_filings  # noqa: E402
from pre_news_trading_surveillance.features.daily import compute_event_market_features  # noqa: E402
from pre_news_trading_surveillance.ingest.models import RawFilingRecord  # noqa: E402
from pre_news_trading_surveillance.publish import snapshot  # noqa: E402
from pre_news_trading_surveillance.publish import store as publish_store  # noqa: E402
from pre_news_trading_surveillance.publish.store import PublishedSnapshotStore, RemotePublishedSnapshotStore  # noqa: E402
from pre_news_trading_surveillance.publish.storage import upload_directory_to_s3  # noqa: E402
from pre_news_trading_surveillance.scoring.rules import score_event_detail  # noqa: E402


class PublishSnapshotTests(unittest.TestCase):
    def test_snapshot_bundle_round_trip_and_store_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "test.duckdb"
            schema_dir = Path(__file__).resolve().parents[1] / "sql"
            db.init_database(db_path=db_path, schema_dir=schema_dir)

            self._seed_ranked_event(db_path)
            self._seed_backtest_run(db_path)

            bundle = snapshot.build_snapshot_bundle(db_path=db_path, events_limit=25)
            output_dir = snapshot.write_snapshot_bundle(bundle, root / "publish" / "current")

            manifest = snapshot.load_snapshot_manifest(output_dir)
            summary = snapshot.load_snapshot_summary(output_dir)
            evaluation_summary = snapshot.load_snapshot_evaluation_summary(output_dir)
            model_summary = snapshot.load_snapshot_model_summary(output_dir)
            events = snapshot.load_snapshot_events(output_dir)

            self.assertEqual(manifest["events_count"], 1)
            self.assertFalse(manifest["policy"]["public_safe_mode"])
            self.assertEqual(manifest["evaluation_status"], "available")
            self.assertEqual(manifest["model_status"], "pending")
            self.assertEqual(summary["overview"]["total_events"], 1)
            self.assertEqual(summary["model"]["status"], "pending")
            self.assertEqual(evaluation_summary["status"], "available")
            self.assertEqual(model_summary["status"], "pending")
            self.assertEqual(events["count"], 1)

            store = PublishedSnapshotStore(output_dir)
            self.assertTrue(store.is_available())
            self.assertEqual(store.summary()["overview"]["tracked_tickers"], 1)
            self.assertEqual(store.evaluation_summary()["hybrid"]["top_decile_lift"], 2.7)
            self.assertEqual(len(store.list_events(limit=10, offset=0, ticker="AAPL")), 1)
            self.assertEqual(len(store.list_events(limit=10, ticker="MSFT")), 0)

            event_id = events["items"][0]["event_id"]
            self.assertEqual(store.get_event(event_id)["ticker"], "AAPL")

    def test_remote_snapshot_store_fetches_and_caches_assets(self) -> None:
        calls: list[str] = []
        payloads = {
            "https://example.com/snapshot/manifest.json": {"generated_at": "2026-04-01T10:00:00+00:00"},
            "https://example.com/snapshot/summary.json": {"overview": {"total_events": 1}},
            "https://example.com/snapshot/evaluation_summary.json": {"status": "available", "hybrid": {"top_decile_lift": 2.9}},
            "https://example.com/snapshot/events.json": {
                "items": [
                    {
                        "event_id": "evt-1",
                        "ticker": "AAPL",
                        "event_type": "earnings",
                        "first_public_at": "2024-01-15T13:30:00+00:00",
                        "suspiciousness_score": 72.5,
                    }
                ]
            },
            "https://example.com/snapshot/events/evt-1.json": {
                "event_id": "evt-1",
                "ticker": "AAPL",
                "event_type": "earnings",
                "first_public_at": "2024-01-15T13:30:00+00:00",
                "suspiciousness_score": 72.5,
            },
        }

        class DummyResponse:
            def __init__(self, body: str) -> None:
                self._body = body.encode("utf-8")

            def read(self) -> bytes:
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

        def fake_urlopen(request, timeout=0):  # noqa: ARG001
            url = request.full_url
            calls.append(url)
            return DummyResponse(json.dumps(payloads[url]))

        publish_store._REMOTE_JSON_CACHE.clear()
        with patch("pre_news_trading_surveillance.publish.store.urllib_request.urlopen", side_effect=fake_urlopen):
            store = RemotePublishedSnapshotStore(
                base_url="https://example.com/snapshot",
                cache_ttl_seconds=3600,
            )
            self.assertTrue(store.is_available())
            self.assertEqual(store.manifest()["generated_at"], "2026-04-01T10:00:00+00:00")
            self.assertEqual(store.summary()["overview"]["total_events"], 1)
            self.assertEqual(store.evaluation_summary()["status"], "available")
            self.assertEqual(store.count_events(), 1)
            self.assertEqual(store.list_events(limit=10)[0]["ticker"], "AAPL")
            self.assertEqual(store.get_event("evt-1")["ticker"], "AAPL")
            self.assertEqual(store.manifest()["generated_at"], "2026-04-01T10:00:00+00:00")

        self.assertEqual(calls.count("https://example.com/snapshot/manifest.json"), 1)

    def test_upload_directory_to_s3_uses_relative_keys(self) -> None:
        uploads: list[tuple[str, str, str]] = []

        class DummyClient:
            def upload_file(self, filename, bucket, key, ExtraArgs=None):
                uploads.append((filename, bucket, key))

        dummy_boto3 = types.SimpleNamespace(client=lambda *_args, **_kwargs: DummyClient())

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "summary.json").write_text("{}", encoding="utf-8")
            events_dir = root / "events"
            events_dir.mkdir()
            (events_dir / "evt-1.json").write_text("{}", encoding="utf-8")

            with patch.dict(sys.modules, {"boto3": dummy_boto3}):
                keys = upload_directory_to_s3(
                    source_dir=root,
                    bucket="unit-test-bucket",
                    prefix="public/current",
                )

        self.assertEqual(keys, ["public/current/events/evt-1.json", "public/current/summary.json"])
        self.assertEqual(uploads[0][1], "unit-test-bucket")

    def _seed_ranked_event(self, db_path: Path) -> None:
        filing = RawFilingRecord(
            filing_id="0001:publish",
            ticker="AAPL",
            cik="0000320193",
            company_name="Apple Inc.",
            accession_no="0000320193-24-000003",
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
        events = build_canonical_events_from_filings([filing])
        bars = [
            self._build_bar(day)
            for day in range(2, 15)
        ]
        features = compute_event_market_features(events, bars)

        db.upsert_events(db_path, events)
        db.upsert_event_market_features(db_path, features)

        detail = db.get_ranked_event(db_path, events[0].event_id)
        assert detail is not None
        score = score_event_detail(detail)
        db.upsert_event_scores(db_path, [score])

    def _seed_backtest_run(self, db_path: Path) -> None:
        db.record_ingestion_run(
            db_path=db_path,
            pipeline_name="run_backtest",
            status="success",
            row_count=24,
            metadata={
                "reviewed_events": 24,
                "benchmark_summary": {
                    "reviewed_events": 24,
                    "positive_labels": 8,
                    "control_labels": 16,
                    "fold_count": 3,
                    "k_values": [5, 10, 25],
                    "contamination": 0.12,
                    "ranker_enabled": True,
                },
                "overall_metrics": {
                    "engines": {
                        "hybrid": {
                            "precision_at": {"5": 0.8, "10": 0.6, "25": 0.4},
                            "top_decile_lift": 2.7,
                            "evaluated_events": 24,
                        }
                    },
                    "ablations": [],
                },
            },
            artifact_paths=["/tmp/backtest_report.json"],
        )

    def _build_bar(self, day: int):
        from pre_news_trading_surveillance.domain import MarketBarDaily

        return MarketBarDaily(
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


if __name__ == "__main__":
    unittest.main()
