from __future__ import annotations

import os
import sys
import tempfile
import unittest
from argparse import Namespace
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pre_news_trading_surveillance import db  # noqa: E402
from pre_news_trading_surveillance.cli import cmd_ingest_market_minute  # noqa: E402
from pre_news_trading_surveillance.ingest import market  # noqa: E402
from pre_news_trading_surveillance.settings import default_paths  # noqa: E402


class MarketProviderTests(unittest.TestCase):
    def test_parse_alpha_vantage_intraday_csv_converts_eastern_to_utc(self) -> None:
        text = "\n".join(
            [
                "timestamp,open,high,low,close,volume",
                "2024-01-15 09:30:00,100,101,99.5,100.5,1500",
            ]
        )

        bars = market.parse_alpha_vantage_intraday_csv(
            text,
            symbol="AAPL",
            source="alpha_vantage_api",
            ingested_at="2024-01-15T15:00:00+00:00",
        )

        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].bar_start, "2024-01-15T14:30:00+00:00")
        self.assertEqual(bars[0].ticker, "AAPL")

    def test_fetch_alpha_vantage_daily_csv_raises_for_json_note(self) -> None:
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"Note": "Rate limit exceeded"}'

        with patch("pre_news_trading_surveillance.ingest.market.urlopen", return_value=Response()):
            with self.assertRaisesRegex(market.MarketProviderError, "Rate limit exceeded"):
                market.fetch_alpha_vantage_daily_csv("AAPL", api_key="demo", retry_attempts=1)

    def test_fetch_alpha_vantage_daily_csv_retries_after_rate_limit_note(self) -> None:
        class Response:
            def __init__(self, body: bytes):
                self._body = body

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return self._body

        metrics: dict[str, object] = {}
        with patch(
            "pre_news_trading_surveillance.ingest.market.urlopen",
            side_effect=[
                Response(b'{"Note": "Rate limit exceeded"}'),
                Response(b"timestamp,open,high,low,close,volume\n2024-01-15,1,2,1,2,100\n"),
            ],
        ):
            text = market.fetch_alpha_vantage_daily_csv(
                "AAPL",
                api_key="demo",
                retry_attempts=2,
                retry_backoff_seconds=0,
                metrics=metrics,
            )

        self.assertIn("timestamp,open,high,low,close,volume", text)
        self.assertEqual(metrics["attempt_count"], 2)
        self.assertTrue(metrics["rate_limited"])

    def test_cmd_ingest_market_minute_provider_persists_rows_and_raw_snapshot(self) -> None:
        intraday_csv = "\n".join(
            [
                "timestamp,open,high,low,close,volume",
                "2024-01-15 09:30:00,100,101,99.5,100.5,1500",
                "2024-01-15 09:31:00,100.5,101.2,100.2,101.0,1700",
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = replace(
                default_paths(root=root),
                sql_dir=Path(__file__).resolve().parents[1] / "sql",
            )
            paths.ensure_directories()
            db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)

            args = Namespace(
                csv=None,
                provider=market.ALPHA_VANTAGE_PROVIDER,
                tickers=["AAPL"],
                source="csv_import",
                api_key=None,
                api_key_env="TEST_ALPHA_VANTAGE_KEY",
                interval="1min",
                outputsize="compact",
                month=None,
                entitlement=None,
                adjusted=False,
                extended_hours=True,
                timeout_seconds=30,
            )

            with patch.dict(os.environ, {"TEST_ALPHA_VANTAGE_KEY": "demo"}, clear=False):
                with patch("pre_news_trading_surveillance.cli.default_paths", return_value=paths):
                    with patch(
                        "pre_news_trading_surveillance.ingest.market.fetch_alpha_vantage_intraday_csv",
                        return_value=intraday_csv,
                    ):
                        exit_code = cmd_ingest_market_minute(args)

            self.assertEqual(exit_code, 0)
            bars = db.load_market_bars_minute(paths.db_path, ticker="AAPL")
            self.assertEqual(len(bars), 2)

            snapshots = sorted((paths.raw_dir / "market" / "alpha_vantage" / "minute").glob("AAPL_*.csv"))
            self.assertEqual(len(snapshots), 1)
            self.assertIn("timestamp,open,high,low,close,volume", snapshots[0].read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
