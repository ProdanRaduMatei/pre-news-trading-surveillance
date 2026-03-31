from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pre_news_trading_surveillance import db  # noqa: E402
from pre_news_trading_surveillance.events.sec_events import build_canonical_events_from_filings  # noqa: E402
from pre_news_trading_surveillance.features import minute as minute_features  # noqa: E402
from pre_news_trading_surveillance.ingest.models import RawFilingRecord  # noqa: E402
from pre_news_trading_surveillance.scoring.rules import score_event_detail  # noqa: E402


class MinutePipelineTests(unittest.TestCase):
    def test_load_market_bars_from_csv_normalizes_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "minute.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["ticker", "timestamp", "open", "high", "low", "close", "volume"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "ticker": "aapl",
                        "timestamp": "2024-01-15 14:29:00",
                        "open": "100",
                        "high": "101",
                        "low": "99.5",
                        "close": "100.5",
                        "volume": "1200",
                    }
                )

            bars = minute_features.load_market_bars_from_csv(csv_path, source="test")

        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].ticker, "AAPL")
        self.assertEqual(bars[0].bar_start, "2024-01-15T14:29:00+00:00")
        self.assertEqual(bars[0].trading_date, "2024-01-15")

    def test_build_minute_features_and_score(self) -> None:
        filing = RawFilingRecord(
            filing_id="0001:minute",
            ticker="AAPL",
            cik="0000320193",
            company_name="Apple Inc.",
            accession_no="0000320193-24-000002",
            form_type="8-K",
            filing_date="2024-01-15",
            accepted_at="2024-01-15T14:30:00+00:00",
            items_json='["2.01", "9.01"]',
            primary_document="deal.htm",
            primary_doc_description="Completion of Acquisition",
            source_url="https://example.com/deal.htm",
            raw_path="/tmp/raw-minute.json",
            ingested_at="2024-01-15T14:31:00+00:00",
        )
        events = build_canonical_events_from_filings([filing])
        bars = self._build_minute_bars()
        features = minute_features.compute_event_market_features(events, bars)

        self.assertEqual(len(features), 1)
        self.assertIsNotNone(features[0].pre_15m_return)
        self.assertIsNotNone(features[0].pre_60m_return)
        self.assertIsNotNone(features[0].pre_240m_return)
        self.assertIsNotNone(features[0].volume_z_15m)
        self.assertIsNotNone(features[0].last_bar_at)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            db_path = root / "test.duckdb"
            schema_dir = Path(__file__).resolve().parents[1] / "sql"
            db.init_database(db_path=db_path, schema_dir=schema_dir)
            db.upsert_events(db_path, events)
            db.upsert_market_bars_minute(db_path, bars)
            loaded_bars = db.load_market_bars_minute(db_path, ticker="AAPL")
            self.assertEqual(len(loaded_bars), len(bars))

            db.upsert_event_market_features_minute(db_path, features)
            detail = db.get_ranked_event(db_path, events[0].event_id)
            self.assertIsNotNone(detail)

            score = score_event_detail(detail or {})
            payload = json.loads(score.explanation_payload)

        self.assertGreater(score.suspiciousness_score, 0.0)
        self.assertIn("intraday_component", payload["components"])
        self.assertIn("pre_15m_return", payload["signals"])
        self.assertIn("volume_z_15m", payload["signals"])

    def _build_minute_bars(self):
        from pre_news_trading_surveillance.domain import MarketBarMinute

        start = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        bars = []
        for minute_index in range(300):
            bar_start = start + timedelta(minutes=minute_index)
            base_price = 100 + minute_index * 0.01
            volume = 1_000 + minute_index * 3
            if minute_index >= 240:
                base_price += (minute_index - 239) * 0.05
                volume += 2_500
            bars.append(
                MarketBarMinute(
                    bar_id=f"AAPL:{bar_start.replace(microsecond=0).isoformat()}",
                    ticker="AAPL",
                    bar_start=bar_start.replace(microsecond=0).isoformat(),
                    trading_date=bar_start.date().isoformat(),
                    open=base_price,
                    high=base_price + 0.08,
                    low=base_price - 0.05,
                    close=base_price + 0.03,
                    volume=volume,
                    source="test",
                    ingested_at="2024-01-15T15:00:00+00:00",
                )
            )
        return bars


if __name__ == "__main__":
    unittest.main()
