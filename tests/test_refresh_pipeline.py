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

[issuer_releases]
enabled = true
config_path = "configs/issuer_feeds.example.toml"
config_path_env = "TEST_ISSUER_FEED_CONFIG"
user_agent_env = "TEST_ISSUER_UA"
per_feed_limit = 15

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

[model]
enabled = true
output_dir = "data/models/scoring/current"
contamination = 0.15
min_samples = 18
use_ranker = false
review_status = "reviewed"
benchmark_labels = ["suspicious", "control"]

[evaluation]
enabled = true
review_status = "reviewed"
benchmark_labels = ["suspicious", "control"]
folds = 4
min_train_size = 30
k_values = [5, 20]
contamination = 0.11
use_ranker = false
output_dir = "reports/evaluation"

[publish]
enabled = true
output_dir = "data/publish/current"
max_events = 75
public_safe_mode = true
public_delay_minutes = 2880
s3_enabled = false
s3_bucket_env = "TEST_PUBLISH_BUCKET"
                """.strip(),
                encoding="utf-8",
            )

            config = refresh.load_refresh_config(config_path)

        self.assertEqual(config.tickers, ["AAPL", "MSFT"])
        self.assertEqual(config.sec.user_agent_env, "TEST_SEC_UA")
        self.assertFalse(config.sec.refresh_reference)
        self.assertEqual(config.sec.per_ticker_limit, 12)
        self.assertTrue(config.issuer_releases.enabled)
        self.assertEqual(config.issuer_releases.config_path, "configs/issuer_feeds.example.toml")
        self.assertEqual(config.issuer_releases.user_agent_env, "TEST_ISSUER_UA")
        self.assertEqual(config.issuer_releases.per_feed_limit, 15)
        self.assertEqual(config.market.api_key_env, "TEST_AV_KEY")
        self.assertEqual(config.market.timeout_seconds, 45)
        self.assertEqual(config.market_daily.outputsize, "full")
        self.assertFalse(config.market_minute.enabled)
        self.assertEqual(config.market_minute.interval, "5min")
        self.assertEqual(config.nlp.sentiment_backend, "finbert")
        self.assertTrue(config.model.enabled)
        self.assertEqual(config.model.min_samples, 18)
        self.assertFalse(config.model.use_ranker)
        self.assertEqual(config.evaluation.folds, 4)
        self.assertEqual(config.evaluation.k_values, [5, 20])
        self.assertTrue(config.publish.enabled)
        self.assertEqual(config.publish.output_dir, "data/publish/current")
        self.assertEqual(config.publish.max_events, 75)
        self.assertTrue(config.publish.public_safe_mode)
        self.assertEqual(config.publish.public_delay_minutes, 2880)
        self.assertEqual(config.publish.s3_bucket_env, "TEST_PUBLISH_BUCKET")

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
                issuer_releases=refresh.IssuerReleaseRefreshConfig(
                    enabled=True,
                    config_path="configs/issuer_feeds.example.toml",
                    config_path_env="PNTS_ISSUER_FEED_CONFIG",
                    user_agent="Refresh Test Agent/1.0",
                    user_agent_env="PRESS_RELEASES_USER_AGENT",
                    per_feed_limit=12,
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
                model=refresh.ModelRefreshConfig(
                    enabled=True,
                    output_dir="data/models/scoring/current",
                    contamination=0.12,
                    min_samples=4,
                    use_ranker=True,
                    review_status="reviewed",
                    benchmark_labels=["suspicious", "control"],
                    reviewer=None,
                ),
                evaluation=refresh.EvaluationRefreshConfig(
                    enabled=True,
                    review_status="reviewed",
                    benchmark_labels=["suspicious", "control"],
                    reviewer=None,
                    folds=2,
                    min_train_size=2,
                    k_values=[5, 10],
                    contamination=0.12,
                    use_ranker=True,
                    output_dir="reports/evaluation",
                ),
                publish=refresh.PublishRefreshConfig(
                    enabled=True,
                    output_dir="data/publish/current",
                    max_events=25,
                    public_safe_mode=True,
                    public_delay_minutes=1440,
                    s3_enabled=False,
                    s3_bucket=None,
                    s3_bucket_env="PUBLISH_S3_BUCKET",
                    s3_prefix="current",
                    s3_region=None,
                    s3_region_env="PUBLISH_S3_REGION",
                    s3_endpoint_url=None,
                    s3_endpoint_url_env="PUBLISH_S3_ENDPOINT_URL",
                    s3_access_key_env="AWS_ACCESS_KEY_ID",
                    s3_secret_key_env="AWS_SECRET_ACCESS_KEY",
                    s3_session_token_env="AWS_SESSION_TOKEN",
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

                def cmd_ingest_press_releases(self, _args):
                    self.calls.append("press_releases")
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

                def cmd_train_model_stack(self, _args):
                    self.calls.append("train_model")
                    return 0

                def cmd_score_events(self, _args):
                    self.calls.append("score")
                    return 0

                def cmd_run_backtest(self, _args):
                    self.calls.append("backtest")
                    return 0

                def cmd_publish_snapshot(self, _args):
                    self.calls.append("publish")
                    return 0

            cli_module = DummyCli()

            db.record_ingestion_run(
                db_path=paths.db_path,
                pipeline_name="score_events",
                status="success",
                row_count=4,
            )
            duckdb = db._require_duckdb()
            connection = duckdb.connect(str(paths.db_path))
            try:
                connection.execute(
                    """
                    INSERT INTO events (
                      event_id, source_event_id, source_table, ticker, issuer_name, first_public_at, event_date,
                      event_type, sentiment_label, sentiment_score, title, summary, source_url, primary_document,
                      sec_items_json, official_source_flag, timestamp_confidence, classifier_backend,
                      sentiment_backend, novelty_backend, source_quality, novelty, impact_score, built_at
                    )
                    VALUES
                      ('evt-1','src-1','raw_filings','AAPL','Apple Inc.','2024-01-01T10:00:00+00:00','2024-01-01','earnings','positive',0.8,'Title 1','Summary 1','https://example.com/1','doc1','[]',true,'high','rules','heuristic','lexical',0.9,0.2,0.8,'2024-01-01T10:05:00+00:00'),
                      ('evt-2','src-2','raw_filings','MSFT','Microsoft Corp.','2024-01-02T10:00:00+00:00','2024-01-02','earnings','negative',-0.4,'Title 2','Summary 2','https://example.com/2','doc2','[]',true,'high','rules','heuristic','lexical',0.9,0.3,0.6,'2024-01-02T10:05:00+00:00'),
                      ('evt-3','src-3','raw_filings','AAPL','Apple Inc.','2024-01-03T10:00:00+00:00','2024-01-03','mna','positive',0.5,'Title 3','Summary 3','https://example.com/3','doc3','[]',true,'high','rules','heuristic','lexical',0.9,0.6,0.9,'2024-01-03T10:05:00+00:00'),
                      ('evt-4','src-4','raw_filings','MSFT','Microsoft Corp.','2024-01-04T10:00:00+00:00','2024-01-04','other','neutral',0.0,'Title 4','Summary 4','https://example.com/4','doc4','[]',true,'high','rules','heuristic','lexical',0.9,0.1,0.2,'2024-01-04T10:05:00+00:00')
                    """
                )
                connection.execute(
                    """
                    INSERT INTO benchmark_event_labels (
                      event_id, benchmark_label, review_status, reviewer, label_source, confidence,
                      review_notes, metadata_json, created_at, updated_at
                    )
                    VALUES
                      ('evt-1','suspicious','reviewed','tester','manual_review',0.9,'','{}','2024-01-04T00:00:00+00:00','2024-01-04T00:00:00+00:00'),
                      ('evt-2','control','reviewed','tester','manual_review',0.9,'','{}','2024-01-04T00:00:00+00:00','2024-01-04T00:00:00+00:00'),
                      ('evt-3','suspicious','reviewed','tester','manual_review',0.9,'','{}','2024-01-04T00:00:00+00:00','2024-01-04T00:00:00+00:00'),
                      ('evt-4','control','reviewed','tester','manual_review',0.9,'','{}','2024-01-04T00:00:00+00:00','2024-01-04T00:00:00+00:00')
                    """
                )
            finally:
                connection.close()

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
