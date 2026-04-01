from __future__ import annotations

import json
import math
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from ..scoring import anomaly_stack, rules

POSITIVE_LABEL = "suspicious"
CONTROL_LABEL = "control"

DEFAULT_ENGINES = ["rules", "nlp_only", "market_only", "anomaly_only", "hybrid"]


@dataclass(frozen=True)
class BacktestArtifacts:
    report: dict[str, object]
    json_path: Path
    markdown_path: Path


def run_backtest(
    details: list[dict[str, object]],
    *,
    output_dir: Path,
    folds: int = 3,
    min_train_size: int = 24,
    k_values: list[int] | None = None,
    contamination: float = 0.12,
    use_ranker: bool = True,
) -> BacktestArtifacts:
    if folds < 1:
        raise ValueError("Backtest requires at least one fold.")
    k_values = sorted({int(value) for value in (k_values or [5, 10, 25]) if int(value) > 0})
    labeled = [
        detail
        for detail in details
        if str(detail.get("benchmark_label", "")).strip().lower() in {POSITIVE_LABEL, CONTROL_LABEL}
    ]
    if len(labeled) < (min_train_size + folds):
        raise ValueError(
            f"Need at least {min_train_size + folds} reviewed benchmark events to run a {folds}-fold backtest; received {len(labeled)}."
        )

    sorted_details = sorted(
        labeled,
        key=lambda item: (
            str(item.get("first_public_at", "")),
            str(item.get("event_id", "")),
        ),
    )
    fold_slices = _build_fold_slices(len(sorted_details), folds=folds, min_train_size=min_train_size)

    fold_reports: list[dict[str, object]] = []
    for fold_index, (test_start, test_end) in enumerate(fold_slices, start=1):
        train_details = sorted_details[:test_start]
        test_details = sorted_details[test_start:test_end]
        period = {
            "train_start": str(train_details[0]["first_public_at"]),
            "train_end": str(train_details[-1]["first_public_at"]),
            "test_start": str(test_details[0]["first_public_at"]),
            "test_end": str(test_details[-1]["first_public_at"]),
        }
        engine_scores = _score_fold(
            train_details,
            test_details,
            contamination=contamination,
            use_ranker=use_ranker,
        )
        fold_reports.append(
            {
                "fold_index": fold_index,
                "train_count": len(train_details),
                "test_count": len(test_details),
                "period": period,
                "base_rate": round(_base_rate(test_details), 4),
                "engines": {
                    engine: _compute_metrics(test_details, scores, k_values=k_values)
                    for engine, scores in engine_scores.items()
                },
            }
        )

    report = _assemble_report(
        details=sorted_details,
        fold_reports=fold_reports,
        k_values=k_values,
        contamination=contamination,
        use_ranker=use_ranker,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = _timestamp_slug()
    json_path = output_dir / f"backtest_report_{timestamp}.json"
    markdown_path = output_dir / f"backtest_report_{timestamp}.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(render_markdown_report(report), encoding="utf-8")
    return BacktestArtifacts(report=report, json_path=json_path, markdown_path=markdown_path)


def render_markdown_report(report: dict[str, object]) -> str:
    benchmark = report["benchmark"]
    overall = report["overall"]
    k_values = [str(value) for value in benchmark["k_values"]]
    lines = [
        "# Backtest Report",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Reviewed benchmark events: `{benchmark['reviewed_events']}`",
        f"- Suspicious labels: `{benchmark['positive_labels']}`",
        f"- Control labels: `{benchmark['control_labels']}`",
        f"- Base rate: `{overall['base_rate']:.4f}`",
        "",
        "## Aggregate Metrics",
        "",
        "| Engine | "
        + " | ".join(f"Precision@{value}" for value in k_values)
        + " | Top-Decile Lift |",
        "| --- | "
        + " | ".join("---:" for _ in k_values)
        + " | ---: |",
    ]

    for engine_name, metrics in overall["engines"].items():
        precision = metrics["precision_at"]
        lines.append(
            "| "
            + " | ".join(
                [
                    engine_name,
                    *[_fmt_metric(precision.get(value)) for value in k_values],
                    _fmt_metric(metrics.get("top_decile_lift")),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Ablations",
            "",
            "| Hybrid vs | "
            + " | ".join(f"Delta Precision@{value}" for value in k_values)
            + " | Delta Top-Decile Lift |",
            "| --- | "
            + " | ".join("---:" for _ in k_values)
            + " | ---: |",
        ]
    )
    for row in overall["ablations"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    row["comparison_engine"],
                    *[_fmt_metric(row["delta_precision_at"].get(value)) for value in k_values],
                    _fmt_metric(row.get("delta_top_decile_lift")),
                ]
            )
            + " |"
        )

    return "\n".join(lines) + "\n"


def _assemble_report(
    *,
    details: list[dict[str, object]],
    fold_reports: list[dict[str, object]],
    k_values: list[int],
    contamination: float,
    use_ranker: bool,
) -> dict[str, object]:
    total_positive = sum(1 for detail in details if _is_positive(detail))
    total_control = sum(1 for detail in details if _is_control(detail))
    engines = DEFAULT_ENGINES
    overall_engine_metrics: dict[str, dict[str, object]] = {}
    for engine in engines:
        overall_engine_metrics[engine] = _aggregate_engine_metrics(
            [fold["engines"][engine] for fold in fold_reports],
            k_values=k_values,
        )

    return {
        "generated_at": _utc_now_iso(),
        "benchmark": {
            "reviewed_events": len(details),
            "positive_labels": total_positive,
            "control_labels": total_control,
            "fold_count": len(fold_reports),
            "k_values": k_values,
            "contamination": contamination,
            "ranker_enabled": use_ranker,
        },
        "overall": {
            "base_rate": round(total_positive / len(details), 4) if details else 0.0,
            "engines": overall_engine_metrics,
            "ablations": _build_ablation_rows(overall_engine_metrics, k_values=k_values),
        },
        "folds": fold_reports,
    }


def _score_fold(
    train_details: list[dict[str, object]],
    test_details: list[dict[str, object]],
    *,
    contamination: float,
    use_ranker: bool,
) -> dict[str, list[float]]:
    return {
        "rules": [rules.score_event_detail(detail).suspiciousness_score for detail in test_details],
        "nlp_only": _score_nlp_only(train_details, test_details),
        "market_only": _score_isolation_fold(
            train_details,
            test_details,
            feature_subset=anomaly_stack.MARKET_FEATURES + anomaly_stack.METADATA_FEATURES,
            contamination=contamination,
        ),
        "anomaly_only": _score_isolation_fold(
            train_details,
            test_details,
            feature_subset=anomaly_stack.feature_names(),
            contamination=contamination,
        ),
        "hybrid": _score_hybrid_fold(
            train_details,
            test_details,
            contamination=contamination,
            use_ranker=use_ranker,
        ),
    }


def _score_nlp_only(
    train_details: list[dict[str, object]],
    test_details: list[dict[str, object]],
) -> list[float]:
    train_raw = [_nlp_prior_raw(detail) for detail in train_details]
    lower = min(train_raw)
    upper = max(train_raw)
    return [round(_normalize_raw(_nlp_prior_raw(detail), lower, upper) * 100, 2) for detail in test_details]


def _score_isolation_fold(
    train_details: list[dict[str, object]],
    test_details: list[dict[str, object]],
    *,
    feature_subset: list[str],
    contamination: float,
) -> list[float]:
    try:
        from sklearn.ensemble import IsolationForest
    except ImportError as exc:
        raise RuntimeError(
            "scikit-learn is required for evaluation backtests. Install it with `pip install -e .[ml]`."
        ) from exc

    train_matrix = anomaly_stack.build_feature_matrix(train_details, feature_subset=feature_subset)
    test_matrix = anomaly_stack.build_feature_matrix(test_details, feature_subset=feature_subset)
    model = IsolationForest(
        n_estimators=200,
        contamination=min(max(contamination, 0.01), 0.4),
        random_state=42,
        n_jobs=1,
    )
    model.fit(train_matrix)
    train_raw = [-float(value) for value in model.score_samples(train_matrix)]
    lower = min(train_raw)
    upper = max(train_raw)
    return [
        round(_normalize_raw(-float(value), lower, upper) * 100, 2)
        for value in model.score_samples(test_matrix)
    ]


def _score_hybrid_fold(
    train_details: list[dict[str, object]],
    test_details: list[dict[str, object]],
    *,
    contamination: float,
    use_ranker: bool,
) -> list[float]:
    with tempfile.TemporaryDirectory() as tmpdir:
        model_dir = Path(tmpdir)
        anomaly_stack.train_model_stack(
            train_details,
            output_dir=model_dir,
            contamination=contamination,
            min_samples=min(12, len(train_details)),
            enable_ranker=use_ranker,
        )
        scores, _metadata = anomaly_stack.score_event_details(
            test_details,
            engine="hybrid",
            model_dir=model_dir,
        )
    return [float(score.suspiciousness_score) for score in scores]


def _compute_metrics(
    details: list[dict[str, object]],
    scores: list[float],
    *,
    k_values: list[int],
) -> dict[str, object]:
    ranked = sorted(
        zip(details, scores),
        key=lambda item: (-float(item[1]), str(item[0].get("event_id", ""))),
    )
    positives = sum(1 for detail, _score in ranked if _is_positive(detail))
    base_rate = positives / len(ranked) if ranked else 0.0
    precision = {str(k): _precision_at_k(ranked, k) for k in k_values}
    top_decile_size = max(1, math.ceil(len(ranked) * 0.1)) if ranked else 0
    top_decile_precision = _precision_at_k(ranked, top_decile_size) if ranked else None
    top_decile_lift = (
        round(top_decile_precision / base_rate, 4)
        if top_decile_precision is not None and base_rate > 0
        else None
    )
    return {
        "evaluated_events": len(ranked),
        "positive_labels": positives,
        "base_rate": round(base_rate, 4),
        "precision_at": precision,
        "top_decile_size": top_decile_size,
        "top_decile_precision": round(top_decile_precision, 4) if top_decile_precision is not None else None,
        "top_decile_lift": top_decile_lift,
    }


def _aggregate_engine_metrics(
    fold_metrics: list[dict[str, object]],
    *,
    k_values: list[int],
) -> dict[str, object]:
    precision_at = {}
    for k in k_values:
        values = [
            float(fold["precision_at"][str(k)])
            for fold in fold_metrics
            if fold["precision_at"].get(str(k)) is not None
        ]
        precision_at[str(k)] = round(sum(values) / len(values), 4) if values else None

    lift_values = [
        float(fold["top_decile_lift"])
        for fold in fold_metrics
        if fold.get("top_decile_lift") is not None
    ]
    return {
        "precision_at": precision_at,
        "top_decile_lift": round(sum(lift_values) / len(lift_values), 4) if lift_values else None,
        "fold_count": len(fold_metrics),
        "evaluated_events": sum(int(fold["evaluated_events"]) for fold in fold_metrics),
    }


def _build_ablation_rows(
    overall_engine_metrics: dict[str, dict[str, object]],
    *,
    k_values: list[int],
) -> list[dict[str, object]]:
    hybrid = overall_engine_metrics["hybrid"]
    rows = []
    for engine_name, metrics in overall_engine_metrics.items():
        if engine_name == "hybrid":
            continue
        delta_precision = {}
        for k in k_values:
            hybrid_value = hybrid["precision_at"].get(str(k))
            candidate_value = metrics["precision_at"].get(str(k))
            delta_precision[str(k)] = (
                round(float(hybrid_value) - float(candidate_value), 4)
                if hybrid_value is not None and candidate_value is not None
                else None
            )
        delta_lift = (
            round(float(hybrid["top_decile_lift"]) - float(metrics["top_decile_lift"]), 4)
            if hybrid.get("top_decile_lift") is not None and metrics.get("top_decile_lift") is not None
            else None
        )
        rows.append(
            {
                "comparison_engine": engine_name,
                "delta_precision_at": delta_precision,
                "delta_top_decile_lift": delta_lift,
            }
        )
    return rows


def _precision_at_k(ranked: list[tuple[dict[str, object], float]], k: int) -> float | None:
    if not ranked or k <= 0:
        return None
    top_k = ranked[: min(k, len(ranked))]
    positives = sum(1 for detail, _score in top_k if _is_positive(detail))
    return round(positives / len(top_k), 4)


def _build_fold_slices(total_count: int, *, folds: int, min_train_size: int) -> list[tuple[int, int]]:
    remaining = total_count - min_train_size
    if remaining < folds:
        raise ValueError("Not enough remaining benchmark events to allocate one test point per fold.")
    base_size = remaining // folds
    remainder = remaining % folds
    start = min_train_size
    slices: list[tuple[int, int]] = []
    for fold_index in range(folds):
        fold_size = base_size + (1 if fold_index < remainder else 0)
        end = start + fold_size
        if fold_size <= 0:
            continue
        slices.append((start, end))
        start = end
    return slices


def _nlp_prior_raw(detail: dict[str, object]) -> float:
    sentiment_magnitude = abs(float(detail.get("sentiment_score") or 0.0))
    impact = float(detail.get("impact_score") or 0.0)
    novelty = float(detail.get("novelty") or 0.0)
    source_quality = float(detail.get("source_quality") or 0.0)
    confidence = anomaly_stack.feature_map(detail)["timestamp_confidence_score"]
    return (
        0.30 * impact
        + 0.25 * novelty
        + 0.20 * source_quality
        + 0.15 * min(sentiment_magnitude, 1.0)
        + 0.10 * confidence
    )


def _normalize_raw(value: float, lower: float, upper: float) -> float:
    if upper <= lower:
        return 0.0
    return max(0.0, min(1.0, (value - lower) / (upper - lower)))


def _base_rate(details: list[dict[str, object]]) -> float:
    return sum(1 for detail in details if _is_positive(detail)) / len(details) if details else 0.0


def _is_positive(detail: dict[str, object]) -> bool:
    return str(detail.get("benchmark_label", "")).strip().lower() == POSITIVE_LABEL


def _is_control(detail: dict[str, object]) -> bool:
    return str(detail.get("benchmark_label", "")).strip().lower() == CONTROL_LABEL


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _fmt_metric(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4f}"
