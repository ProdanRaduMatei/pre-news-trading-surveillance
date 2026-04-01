from __future__ import annotations

import json
from pathlib import Path

from .. import db


def load_public_evaluation_summary(db_path: Path) -> dict[str, object]:
    runs = db.get_latest_successful_runs(db_path, ["run_backtest"])
    return build_public_evaluation_summary(runs.get("run_backtest"))


def build_public_evaluation_summary(run: dict[str, object] | None) -> dict[str, object]:
    if not run:
        return pending_public_evaluation_summary()

    metadata = dict(run.get("metadata") or {})
    report_payload = _load_report_payload(str(metadata.get("json_report_path") or ""))
    benchmark = dict(metadata.get("benchmark_summary") or report_payload.get("benchmark") or {})
    overall = dict(metadata.get("overall_metrics") or report_payload.get("overall") or {})

    if not benchmark and not overall:
        return pending_public_evaluation_summary()

    generated_at = (
        str(report_payload.get("generated_at") or "")
        or str(run.get("finished_at") or "")
        or str(run.get("started_at") or "")
        or None
    )
    benchmark_summary = {
        "reviewed_events": _optional_int(benchmark.get("reviewed_events") or metadata.get("reviewed_events")),
        "positive_labels": _optional_int(benchmark.get("positive_labels")),
        "control_labels": _optional_int(benchmark.get("control_labels")),
        "fold_count": _optional_int(benchmark.get("fold_count") or metadata.get("folds")),
        "k_values": [int(value) for value in benchmark.get("k_values", metadata.get("k_values", []))],
        "contamination": _optional_float(benchmark.get("contamination") or metadata.get("contamination")),
        "ranker_enabled": _optional_bool(
            benchmark.get("ranker_enabled")
            if "ranker_enabled" in benchmark
            else metadata.get("use_ranker")
        ),
    }

    return {
        "status": "available",
        "generated_at": generated_at,
        "benchmark": benchmark_summary,
        "overall": overall,
        "hybrid": dict((overall.get("engines") or {}).get("hybrid") or {}),
        "ablations": list(overall.get("ablations") or []),
        "source": {
            "run_id": run.get("run_id"),
            "finished_at": run.get("finished_at"),
            "report_path": metadata.get("json_report_path"),
            "markdown_report_path": metadata.get("markdown_report_path"),
        },
        "notice": (
            "Metrics are computed on a reviewed benchmark of suspicious versus control events and are "
            "intended to measure ranking quality, not legal truth."
        ),
    }


def pending_public_evaluation_summary() -> dict[str, object]:
    return {
        "status": "pending",
        "generated_at": None,
        "benchmark": {
            "reviewed_events": 0,
            "positive_labels": 0,
            "control_labels": 0,
            "fold_count": 0,
            "k_values": [],
            "contamination": None,
            "ranker_enabled": None,
        },
        "overall": {},
        "hybrid": {},
        "ablations": [],
        "source": {},
        "notice": (
            "No published backtest report is currently available. Review benchmark candidates, import "
            "labels, and run `pnts run-backtest` to publish evaluation metrics."
        ),
    }


def _load_report_payload(path_value: str) -> dict[str, object]:
    if not path_value:
        return {}
    path = Path(path_value)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_bool(value: object) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return bool(value)
