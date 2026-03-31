from __future__ import annotations

import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pre_news_trading_surveillance import db  # noqa: E402
from pre_news_trading_surveillance.pipeline import refresh  # noqa: E402
from pre_news_trading_surveillance.settings import default_paths  # noqa: E402


class RefreshPipelineTests(unittest.TestCase):
    def test_load_refresh_config_reads_nested_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "refresh.toml"
            config_path.write_text(
                """
tickers = ["aapl", "msft"]

[sec]
user_agent_env = "TEST_SEC_UA"
refresh_reference = false
per_ticker_limit = 12
forms = ["8-K"]

[market]
provider = "alpha_vantage"
api_key_env = "TEST_AV_KEY"
timeout_seconds = 45

[market.daily]
enabled = true
outputsize = "full"

[market.minute]
enabled = false
interval = "5min"
outputsize = "compact"
extended_hours = false

[nlp]
sentiment_backend = "finbert"
novelty_backend = "lexical"
                """.strip(),
                encoding="utf-8",
            )

            config = refresh.load_refresh_config(config_path)

        self.assertEqual(config.tickers, ["AAPL", "MSFT"])
        self.assertEqual(config.sec.user_agent_env, "TEST_SEC_UA")
        self.assertFalse(config.sec.refresh_reference)
        self.assertEqual(config.sec.per_ticker_limit, 12)
        self.assertEqual(config.market.api_key_env, "TEST_AV_KEY")
        self.assertEqual(config.market.timeout_seconds, 45)
        self.assertEqual(config.market_daily.outputsize, "full")
        self.assertFalse(config.market_minute.enabled)
        self.assertEqual(config.market_minute.interval, "5min")
        self.assertEqual(config.nlp.sentiment_backend, "finbert")

    def test_resolve_refresh_steps_supports_modes_and_explicit_override(self) -> None:
        self.assertEqual(refresh.resolve_refresh_steps("intraday"), refresh.INTRADAY_REFRESH_STEPS)
        self.assertEqual(
            refresh.resolve_refresh_steps("full", ["market_minute", "score"]),
            ["market_minute", "score"],
        )

    def test_run_refresh_pipeline_invokes_steps_in_order_and_records_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = replace(default_paths(root=root), sql_dir=Path(__file__).resolve().parents[1] / "sql")
            paths.ensure_directories()
            db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)

            config = refresh.RefreshPipelineConfig(
                tickers=["AAPL", "MSFT"],
                sec=refresh.SecRefreshConfig(
                    user_agent="Test Agent test@example.com",
                    user_agent_env="SEC_USER_AGENT",
                    refresh_reference=True,
                    per_ticker_limit=10,
                    forms=["8-K", "6-K"],
                ),
                market=refresh.MarketProviderConfig(
                    provider="alpha_vantage",
                    api_key="demo",
                    api_key_env="ALPHAVANTAGE_API_KEY",
                    timeout_seconds=30,
                ),
                market_daily=refresh.MarketDailyRefreshConfig(enabled=True, outputsize="compact"),
                market_minute=refresh.MarketMinuteRefreshConfig(
                    enabled=True,
                    interval="1min",
                    outputsize="compact",
                    month=None,
                    entitlement=None,
                    adjusted=False,
                    extended_hours=True,
                ),
                nlp=refresh.NlpRefreshConfig(
                    sentiment_backend="heuristic",
                    sentiment_model=None,
                    novelty_backend="lexical",
                    novelty_model=None,
                ),
            )

            class DummyCli:
                def __init__(self):
                    self.calls: list[str] = []

                def cmd_ingest_sec_reference(self, _args):
                    self.calls.append("sec_reference")
                    return 0

                def cmd_ingest_sec_filings(self, _args):
                    self.calls.append("sec_filings")
                    return 0

                def cmd_ingest_market_daily(self, _args):
                    self.calls.append("market_daily")
                    return 0

                def cmd_ingest_market_minute(self, _args):
                    self.calls.append("market_minute")
                    return 0

                def cmd_build_sec_events(self, _args):
                    self.calls.append("build_events")
                    return 0

                def cmd_compute_daily_features(self, _args):
                    self.calls.append("compute_daily")
                    return 0

                def cmd_compute_minute_features(self, _args):
                    self.calls.append("compute_minute")
                    return 0

                def cmd_score_events(self, _args):
                    self.calls.append("score")
                    return 0

            cli_module = DummyCli()
            completed = refresh.run_refresh_pipeline(
                config=config,
                steps=refresh.FULL_REFRESH_STEPS,
                cli_module=cli_module,
                paths=paths,
            )

            self.assertEqual(completed, refresh.FULL_REFRESH_STEPS)
            self.assertEqual(cli_module.calls, refresh.FULL_REFRESH_STEPS)

            duckdb = db._require_duckdb()
            connection = duckdb.connect(str(paths.db_path))
            try:
                row = connection.execute(
                    """
                    SELECT pipeline_name, status, row_count
                    FROM ingestion_runs
                    WHERE pipeline_name = 'refresh_pipeline'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                connection.close()

            self.assertEqual(row, ("refresh_pipeline", "success", len(refresh.FULL_REFRESH_STEPS)))


if __name__ == "__main__":
    unittest.main()
