from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pre_news_trading_surveillance.ingest.sec import (  # noqa: E402
    normalize_acceptance_datetime,
    parse_company_tickers,
    parse_recent_filings,
)


class SecParsingTests(unittest.TestCase):
    def test_parse_company_tickers(self) -> None:
        payload = {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corp"},
        }

        references = parse_company_tickers(payload, retrieved_at="2026-03-31T12:00:00+00:00")

        self.assertEqual(len(references), 2)
        self.assertEqual(references[0].ticker, "AAPL")
        self.assertEqual(references[0].cik, "0000320193")
        self.assertEqual(references[1].ticker, "MSFT")

    def test_parse_recent_filings(self) -> None:
        payload = {
            "cik": "320193",
            "name": "Apple Inc.",
            "filings": {
                "recent": {
                    "accessionNumber": ["0000320193-24-000001"],
                    "filingDate": ["2024-01-02"],
                    "acceptanceDateTime": ["20240102163045"],
                    "form": ["8-K"],
                    "primaryDocument": ["a8k.htm"],
                    "primaryDocDescription": ["Current report"],
                }
            },
        }

        rows = parse_recent_filings(payload=payload, ticker="AAPL", raw_path=Path("/tmp/apple.json"))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].filing_id, "0000320193:0000320193-24-000001")
        self.assertEqual(rows[0].form_type, "8-K")
        self.assertEqual(rows[0].raw_path, "/tmp/apple.json")
        self.assertTrue(rows[0].source_url.endswith("/a8k.htm"))
        self.assertEqual(rows[0].accepted_at, "2024-01-02T16:30:45+00:00")

    def test_normalize_acceptance_datetime_handles_iso_zulu(self) -> None:
        value = "2024-02-10T21:12:05.000Z"
        self.assertEqual(
            normalize_acceptance_datetime(value),
            "2024-02-10T21:12:05+00:00",
        )


if __name__ == "__main__":
    unittest.main()
