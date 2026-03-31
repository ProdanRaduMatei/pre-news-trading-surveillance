from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pre_news_trading_surveillance import db  # noqa: E402
from pre_news_trading_surveillance.domain import MarketBarDaily  # noqa: E402
from pre_news_trading_surveillance.events.sec_events import (  # noqa: E402
    build_canonical_events_from_filings,
    classify_event_type,
)
from pre_news_trading_surveillance.features.daily import compute_event_market_features  # noqa: E402
from pre_news_trading_surveillance.ingest.models import RawFilingRecord  # noqa: E402
from pre_news_trading_surveillance.scoring.rules import score_event_detail  # noqa: E402


class EventPipelineTests(unittest.TestCase):
    def test_classify_event_type_prefers_mna_keywords(self) -> None:
        event_type = classify_event_type(
            form_type="8-K",
            primary_doc_description="Completion of Acquisition",
            primary_document="acquisition.htm",
        )
        self.assertEqual(event_type, "mna")

    def test_build_features_and_score(self) -> None:
        filing = RawFilingRecord(
            filing_id="0001:abc",
            ticker="AAPL",
            cik="0000320193",
            company_name="Apple Inc.",
            accession_no="0000320193-24-000001",
            form_type="8-K",
            filing_date="2024-01-15",
            accepted_at="2024-01-15T13:30:00+00:00",
            primary_document="deal.htm",
            primary_doc_description="Completion of Acquisition",
            source_url="https://example.com/deal.htm",
            raw_path="/tmp/raw.json",
            ingested_at="2024-01-15T13:31:00+00:00",
        )
        events = build_canonical_events_from_filings([filing])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "mna")

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
        features = compute_event_market_features(events, bars)
        self.assertEqual(len(features), 1)
        self.assertIsNotNone(features[0].pre_1d_return)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "test.duckdb"
            schema_dir = Path(__file__).resolve().parents[1] / "sql"
            db.init_database(db_path=db_path, schema_dir=schema_dir)
            db.upsert_events(db_path, events)
            db.upsert_event_market_features(db_path, features)

            detail = db.get_ranked_event(db_path, events[0].event_id)
            self.assertIsNotNone(detail)
            score = score_event_detail(detail or {})
            payload = json.loads(score.explanation_payload)
            self.assertIn("summary", payload)
            self.assertGreaterEqual(score.suspiciousness_score, 0.0)


if __name__ == "__main__":
    unittest.main()
