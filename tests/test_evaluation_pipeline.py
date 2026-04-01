from __future__ import annotations

import csv
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
from pre_news_trading_surveillance.cli import (  # noqa: E402
    cmd_export_benchmark_candidates,
    cmd_import_benchmark_labels,
    cmd_run_backtest,
    cmd_score_events,
)
from pre_news_trading_surveillance.domain import (  # noqa: E402
    CanonicalEvent,
    EventMarketFeature,
    EventMarketFeatureMinute,
)
from pre_news_trading_surveillance.settings import default_paths  # noqa: E402


class EvaluationPipelineTests(unittest.TestCase):
    def test_export_and_import_benchmark_labels(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = self._build_paths(tmpdir)
            event_ids = self._seed_feature_rich_events(paths.db_path, count=16)

            with patch("pre_news_trading_surveillance.cli.default_paths", return_value=paths):
                score_exit = cmd_score_events(
                    Namespace(
                        ticker=None,
                        engine="rules",
                        model_dir=paths.models_dir / "scoring" / "current",
                        parent_run_id=None,
                    )
                )
                self.assertEqual(score_exit, 0)

                review_csv = paths.benchmarks_dir / "review_candidates.csv"
                export_exit = cmd_export_benchmark_candidates(
                    Namespace(
                        ticker=None,
                        output_csv=review_csv,
                        top_k=4,
                        bottom_k=4,
                        parent_run_id=None,
                    )
                )
                self.assertEqual(export_exit, 0)

                with review_csv.open("r", encoding="utf-8", newline="") as handle:
                    rows = list(csv.DictReader(handle))
                self.assertEqual(len(rows), 8)
                self.assertIn(rows[0]["suggested_label"], {"suspicious", "control"})

                rows[0]["review_label"] = "suspicious"
                rows[0]["review_status"] = "reviewed"
                rows[0]["reviewer"] = "unit-test"
                rows[1]["review_label"] = "control"
                rows[1]["review_status"] = "reviewed"
                rows[1]["reviewer"] = "unit-test"
                with review_csv.open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
                    writer.writeheader()
                    writer.writerows(rows)

                import_exit = cmd_import_benchmark_labels(
                    Namespace(
                        csv=review_csv,
                        reviewer=None,
                        label_source="manual_review",
                        parent_run_id=None,
                    )
                )
                self.assertEqual(import_exit, 0)

            labels = db.list_benchmark_labels(paths.db_path, limit=10)
            self.assertEqual(len(labels), 2)
            self.assertEqual({label["event_id"] for label in labels}, {rows[0]["event_id"], rows[1]["event_id"]})
            self.assertIn(event_ids[0], set(event_ids))

    def test_run_backtest_writes_report_with_precision_and_ablations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = self._build_paths(tmpdir)
            event_ids = self._seed_feature_rich_events(paths.db_path, count=36)
            labels_csv = paths.benchmarks_dir / "reviewed_benchmark.csv"
            self._write_reviewed_labels_csv(labels_csv, event_ids)

            with patch("pre_news_trading_surveillance.cli.default_paths", return_value=paths):
                score_exit = cmd_score_events(
                    Namespace(
                        ticker=None,
                        engine="rules",
                        model_dir=paths.models_dir / "scoring" / "current",
                        parent_run_id=None,
                    )
                )
                self.assertEqual(score_exit, 0)

                import_exit = cmd_import_benchmark_labels(
                    Namespace(
                        csv=labels_csv,
                        reviewer="unit-test",
                        label_source="manual_review",
                        parent_run_id=None,
                    )
                )
                self.assertEqual(import_exit, 0)

                backtest_exit = cmd_run_backtest(
                    Namespace(
                        review_status="reviewed",
                        benchmark_labels=["suspicious", "control"],
                        reviewer=None,
                        folds=3,
                        min_train_size=24,
                        k_values=[5, 10, 25],
                        contamination=0.12,
                        use_ranker=True,
                        output_dir=paths.reports_dir / "evaluation",
                        parent_run_id=None,
                    )
                )
                self.assertEqual(backtest_exit, 0)

            reports = sorted((paths.reports_dir / "evaluation").glob("backtest_report_*.json"))
            self.assertTrue(reports)
            report = json.loads(reports[-1].read_text(encoding="utf-8"))
            self.assertEqual(report["benchmark"]["reviewed_events"], 36)
            self.assertEqual(len(report["folds"]), 3)
            self.assertIn("hybrid", report["overall"]["engines"])
            self.assertIn("rules", report["overall"]["engines"])
            self.assertIn("precision_at", report["overall"]["engines"]["hybrid"])
            self.assertIn("5", report["overall"]["engines"]["hybrid"]["precision_at"])
            self.assertIsNotNone(report["overall"]["engines"]["hybrid"]["top_decile_lift"])
            self.assertTrue(report["overall"]["ablations"])

            markdown_reports = sorted((paths.reports_dir / "evaluation").glob("backtest_report_*.md"))
            self.assertTrue(markdown_reports)
            markdown_text = markdown_reports[-1].read_text(encoding="utf-8")
            self.assertIn("Backtest Report", markdown_text)
            self.assertIn("Precision@10", markdown_text)

    def _build_paths(self, tmpdir: str):
        root = Path(tmpdir)
        paths = replace(default_paths(root=root), sql_dir=Path(__file__).resolve().parents[1] / "sql")
        paths.ensure_directories()
        db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
        return paths

    def _write_reviewed_labels_csv(self, csv_path: Path, event_ids: list[str]) -> None:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["event_id", "review_label", "review_status", "reviewer", "confidence", "review_notes"],
            )
            writer.writeheader()
            for index, event_id in enumerate(event_ids):
                writer.writerow(
                    {
                        "event_id": event_id,
                        "review_label": "suspicious" if index % 3 != 0 else "control",
                        "review_status": "reviewed",
                        "reviewer": "unit-test",
                        "confidence": "0.9",
                        "review_notes": f"Reviewed synthetic event {index}",
                    }
                )

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
