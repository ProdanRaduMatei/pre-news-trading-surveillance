from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pre_news_trading_surveillance import db  # noqa: E402
from pre_news_trading_surveillance.api import app as api_app  # noqa: E402
from pre_news_trading_surveillance.events.sec_events import build_canonical_events_from_filings  # noqa: E402
from pre_news_trading_surveillance.features.daily import compute_event_market_features  # noqa: E402
from pre_news_trading_surveillance.ingest.models import RawFilingRecord  # noqa: E402
from pre_news_trading_surveillance.publish import snapshot as publish_snapshot  # noqa: E402
from pre_news_trading_surveillance.scoring.rules import score_event_detail  # noqa: E402
from pre_news_trading_surveillance.settings import default_paths  # noqa: E402


class DashboardApiTests(unittest.TestCase):
    def test_dashboard_root_and_static_assets_render(self) -> None:
        with self._build_client() as client:
            response = client.get("/")
            self.assertEqual(response.status_code, 200)
            self.assertIn("Pre-News Trading Surveillance", response.text)
            self.assertIn("Explore Ranked Events", response.text)

            static_response = client.get("/static/app.js")
            self.assertEqual(static_response.status_code, 200)
            self.assertIn("loadSummary", static_response.text)

    def test_summary_and_event_detail_endpoints_return_ranked_data(self) -> None:
        with self._build_client() as client:
            summary_response = client.get("/summary")
            self.assertEqual(summary_response.status_code, 200)
            summary_payload = summary_response.json()
            self.assertEqual(summary_payload["overview"]["total_events"], 1)
            self.assertEqual(summary_payload["overview"]["tracked_tickers"], 1)
            self.assertEqual(summary_payload["api"]["name"], "Pre-News Trading Surveillance API")
            self.assertEqual(len(summary_payload["score_bands"]), 1)

            events_response = client.get("/events", params={"limit": 10, "min_score": 0})
            self.assertEqual(events_response.status_code, 200)
            events_payload = events_response.json()
            self.assertEqual(events_payload["count"], 1)
            event_id = events_payload["items"][0]["event_id"]

            detail_response = client.get(f"/events/{event_id}")
            self.assertEqual(detail_response.status_code, 200)
            detail_payload = detail_response.json()
            self.assertEqual(detail_payload["ticker"], "AAPL")
            self.assertIn("summary", detail_payload["explanation_payload"])
            self.assertIn("built_at", detail_payload)
            self.assertIn("scored_at", detail_payload)

    def test_ingestion_runs_endpoint_returns_run_history(self) -> None:
        with self._build_client() as client:
            response = client.get("/ingestion-runs", params={"limit": 10})
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["count"], 1)
            self.assertEqual(payload["items"][0]["pipeline_name"], "seed_data")
            self.assertEqual(payload["items"][0]["status"], "success")
            self.assertEqual(payload["items"][0]["artifact_paths"], ["/tmp/seed.json"])

    def test_summary_and_event_detail_can_serve_published_snapshot(self) -> None:
        tempdir = tempfile.TemporaryDirectory()
        root = Path(tempdir.name)
        paths = default_paths(root=root)
        paths.ensure_directories()

        db.init_database(db_path=paths.db_path, schema_dir=Path(__file__).resolve().parents[1] / "sql")
        self._seed_ranked_event(paths.db_path)
        bundle = publish_snapshot.build_snapshot_bundle(db_path=paths.db_path, events_limit=25)
        publish_snapshot.write_snapshot_bundle(bundle, paths.publish_dir / "current")

        with patch("pre_news_trading_surveillance.api.app.default_paths", return_value=paths):
            with patch.dict(
                os.environ,
                {
                    "PNTS_API_DATA_SOURCE": "published",
                    "PNTS_PUBLISHED_DATA_DIR": str(paths.publish_dir / "current"),
                },
                clear=False,
            ):
                with TestClient(api_app.app) as client:
                    summary_response = client.get("/summary")
                    self.assertEqual(summary_response.status_code, 200)
                    self.assertEqual(summary_response.json()["overview"]["total_events"], 1)

                    events_response = client.get("/events")
                    self.assertEqual(events_response.status_code, 200)
                    event_id = events_response.json()["items"][0]["event_id"]

                    detail_response = client.get(f"/events/{event_id}")
                    self.assertEqual(detail_response.status_code, 200)
                    self.assertEqual(detail_response.json()["ticker"], "AAPL")

        tempdir.cleanup()

    def _build_client(self) -> TestClient:
        tempdir = tempfile.TemporaryDirectory()
        root = Path(tempdir.name)
        paths = default_paths(root=root)
        paths.ensure_directories()

        db.init_database(db_path=paths.db_path, schema_dir=Path(__file__).resolve().parents[1] / "sql")
        self._seed_ranked_event(paths.db_path)
        db.record_ingestion_run(
            db_path=paths.db_path,
            pipeline_name="seed_data",
            status="success",
            row_count=1,
            metadata={"source": "unit-test"},
            artifact_paths=["/tmp/seed.json"],
        )

        patcher = patch("pre_news_trading_surveillance.api.app.default_paths", return_value=paths)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(tempdir.cleanup)
        return TestClient(api_app.app)

    def _seed_ranked_event(self, db_path: Path) -> None:
        filing = RawFilingRecord(
            filing_id="0001:abc",
            ticker="AAPL",
            cik="0000320193",
            company_name="Apple Inc.",
            accession_no="0000320193-24-000001",
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
