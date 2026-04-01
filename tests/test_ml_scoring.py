from __future__ import annotations

import json
import sys
import tempfile
import unittest
from argparse import Namespace
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pre_news_trading_surveillance import db  # noqa: E402
from pre_news_trading_surveillance.cli import cmd_score_events, cmd_train_model_stack  # noqa: E402
from pre_news_trading_surveillance.domain import (  # noqa: E402
    CanonicalEvent,
    EventMarketFeature,
    EventMarketFeatureMinute,
)
from pre_news_trading_surveillance.settings import default_paths  # noqa: E402


class MlScoringTests(unittest.TestCase):
    def test_train_model_stack_writes_bundle_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = self._build_paths(tmpdir)
            self._seed_feature_rich_events(paths.db_path, count=16)

            with patch("pre_news_trading_surveillance.cli.default_paths", return_value=paths):
                exit_code = cmd_train_model_stack(
                    Namespace(
                        ticker=None,
                        output_dir=paths.models_dir / "scoring" / "current",
                        contamination=0.12,
                        min_samples=12,
                        use_ranker=True,
                        parent_run_id=None,
                    )
                )

            self.assertEqual(exit_code, 0)
            manifest_path = paths.models_dir / "scoring" / "current" / "manifest.json"
            bundle_path = paths.models_dir / "scoring" / "current" / "model_bundle.pkl"
            self.assertTrue(manifest_path.exists())
            self.assertTrue(bundle_path.exists())

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["samples"], 16)
            self.assertEqual(manifest["baseline"], "rules")
            self.assertEqual(manifest["ranker_status"], "trained")

    def test_score_events_hybrid_uses_model_stack(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = self._build_paths(tmpdir)
            event_ids = self._seed_feature_rich_events(paths.db_path, count=16)

            with patch("pre_news_trading_surveillance.cli.default_paths", return_value=paths):
                cmd_train_model_stack(
                    Namespace(
                        ticker=None,
                        output_dir=paths.models_dir / "scoring" / "current",
                        contamination=0.12,
                        min_samples=12,
                        use_ranker=True,
                        parent_run_id=None,
                    )
                )
                exit_code = cmd_score_events(
                    Namespace(
                        ticker=None,
                        engine="hybrid",
                        model_dir=paths.models_dir / "scoring" / "current",
                        parent_run_id=None,
                    )
                )

            self.assertEqual(exit_code, 0)
            detail = db.get_ranked_event(paths.db_path, event_ids[0])
            assert detail is not None
            payload = detail["explanation_payload"]
            self.assertEqual(payload["model_stack"]["engine"], "hybrid")
            self.assertIn("anomaly_score", payload["model_stack"])
            self.assertIn("ranker_score", payload["model_stack"])
            self.assertGreaterEqual(float(detail["suspiciousness_score"]), 0.0)

    def test_score_events_auto_falls_back_to_rules_without_model_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = self._build_paths(tmpdir)
            event_ids = self._seed_feature_rich_events(paths.db_path, count=8)

            with patch("pre_news_trading_surveillance.cli.default_paths", return_value=paths):
                exit_code = cmd_score_events(
                    Namespace(
                        ticker=None,
                        engine="auto",
                        model_dir=paths.models_dir / "scoring" / "missing",
                        parent_run_id=None,
                    )
                )

            self.assertEqual(exit_code, 0)
            detail = db.get_ranked_event(paths.db_path, event_ids[0])
            assert detail is not None
            payload = detail["explanation_payload"]
            self.assertEqual(payload["model_stack"]["engine"], "rules")
            self.assertIn("fallback_reason", payload["model_stack"])

    def _build_paths(self, tmpdir: str):
        root = Path(tmpdir)
        paths = replace(default_paths(root=root), sql_dir=Path(__file__).resolve().parents[1] / "sql")
        paths.ensure_directories()
        db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
        return paths

    def _seed_feature_rich_events(self, db_path: Path, *, count: int) -> list[str]:
        events: list[CanonicalEvent] = []
        daily_features: list[EventMarketFeature] = []
        minute_features: list[EventMarketFeatureMinute] = []
        event_ids: list[str] = []

        for index in range(count):
            event_id = f"evt-{index:03d}"
            event_ids.append(event_id)
            positive = index % 3 != 0
            base = index + 1
            sentiment_score = 0.35 + (index * 0.02) if positive else -(0.3 + index * 0.015)
            sentiment_label = "positive" if positive else "negative"
            event_type = "earnings" if index % 2 == 0 else "mna"
            first_public_at = f"2024-02-{(index % 20) + 1:02d}T14:{index % 60:02d}:00+00:00"
            events.append(
                CanonicalEvent(
                    event_id=event_id,
                    source_event_id=f"source-{index}",
                    source_table="raw_filings" if index % 2 == 0 else "raw_issuer_releases",
                    ticker="AAPL" if index < count // 2 else "MSFT",
                    issuer_name="Apple Inc." if index < count // 2 else "Microsoft Corporation",
                    first_public_at=first_public_at,
                    event_date=first_public_at[:10],
                    event_type=event_type,
                    sentiment_label=sentiment_label,
                    sentiment_score=sentiment_score,
                    title=f"Event {index}",
                    summary=f"Synthetic event {index}",
                    source_url=f"https://example.com/event-{index}",
                    primary_document=None,
                    sec_items_json=None,
                    official_source_flag=True,
                    timestamp_confidence="high",
                    classifier_backend="test_rules",
                    sentiment_backend="heuristic",
                    novelty_backend="lexical",
                    source_quality=1.0 if index % 2 == 0 else 0.95,
                    novelty=min(0.95, 0.1 + index * 0.04),
                    impact_score=0.5 + (index * 0.02),
                    built_at="2024-03-01T00:00:00+00:00",
                )
            )
            daily_features.append(
                EventMarketFeature(
                    event_id=event_id,
                    ticker="AAPL" if index < count // 2 else "MSFT",
                    as_of_date=first_public_at[:10],
                    pre_1d_return=(0.01 * base) if positive else -(0.008 * base),
                    pre_5d_return=(0.015 * base) if positive else -(0.012 * base),
                    pre_20d_return=(0.02 * base) if positive else -(0.014 * base),
                    volume_z_1d=0.4 + base * 0.18,
                    volume_z_5d=0.2 + base * 0.12,
                    volatility_20d=0.01 + base * 0.001,
                    gap_pct=(0.003 * base) if positive else -(0.002 * base),
                    avg_volume_20d=1_000_000 + base * 10_000,
                    bars_used=25,
                    computed_at="2024-03-01T00:10:00+00:00",
                )
            )
            minute_features.append(
                EventMarketFeatureMinute(
                    event_id=event_id,
                    ticker="AAPL" if index < count // 2 else "MSFT",
                    as_of_timestamp=first_public_at,
                    pre_15m_return=(0.0025 * base) if positive else -(0.002 * base),
                    pre_60m_return=(0.004 * base) if positive else -(0.003 * base),
                    pre_240m_return=(0.006 * base) if positive else -(0.004 * base),
                    volume_z_15m=0.3 + base * 0.14,
                    volume_z_60m=0.2 + base * 0.1,
                    realized_vol_60m=0.001 + base * 0.0002,
                    range_pct_60m=0.004 + base * 0.0007,
                    last_bar_at=first_public_at,
                    bars_used=240,
                    computed_at="2024-03-01T00:20:00+00:00",
                )
            )

        db.upsert_events(db_path, events)
        db.upsert_event_market_features(db_path, daily_features)
        db.upsert_event_market_features_minute(db_path, minute_features)
        return event_ids


if __name__ == "__main__":
    unittest.main()
