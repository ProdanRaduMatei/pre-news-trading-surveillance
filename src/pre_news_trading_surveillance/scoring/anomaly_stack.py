from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import rules

MODEL_BUNDLE_FILENAME = "model_bundle.pkl"
MODEL_MANIFEST_FILENAME = "manifest.json"
POSITIVE_BENCHMARK_LABEL = "suspicious"
CONTROL_BENCHMARK_LABEL = "control"

NUMERIC_FEATURES = [
    "sentiment_score",
    "novelty",
    "impact_score",
    "source_quality",
    "pre_15m_return",
    "pre_60m_return",
    "pre_240m_return",
    "pre_1d_return",
    "pre_5d_return",
    "pre_20d_return",
    "volume_z_15m",
    "volume_z_60m",
    "volume_z_1d",
    "volume_z_5d",
    "realized_vol_60m",
    "range_pct_60m",
    "volatility_20d",
    "gap_pct",
    "avg_volume_20d",
    "bars_used",
    "minute_bars_used",
]

NLP_FEATURES = [
    "sentiment_score",
    "novelty",
    "impact_score",
    "source_quality",
    "sentiment_direction",
    "timestamp_confidence_score",
    "official_source_flag",
]

MARKET_FEATURES = [
    "pre_15m_return",
    "pre_60m_return",
    "pre_240m_return",
    "pre_1d_return",
    "pre_5d_return",
    "pre_20d_return",
    "volume_z_15m",
    "volume_z_60m",
    "volume_z_1d",
    "volume_z_5d",
    "realized_vol_60m",
    "range_pct_60m",
    "volatility_20d",
    "gap_pct",
    "avg_volume_20d",
    "bars_used",
    "minute_bars_used",
]

METADATA_FEATURES = [
    "source_is_press_release",
    "source_is_sec_filing",
    "event_type_hash",
]


@dataclass(frozen=True)
class TrainingArtifacts:
    output_dir: Path
    manifest_path: Path
    bundle_path: Path
    manifest: dict[str, object]


def train_model_stack(
    details: list[dict[str, object]],
    *,
    output_dir: Path,
    contamination: float = 0.12,
    min_samples: int = 12,
    enable_ranker: bool = True,
) -> TrainingArtifacts:
    if len(details) < min_samples:
        raise ValueError(
            f"Need at least {min_samples} events to train the anomaly stack; received {len(details)}."
        )

    np = _require_numpy()
    IsolationForest = _require_isolation_forest()
    sorted_details = sorted(
        details,
        key=lambda item: (
            str(item.get("ticker", "")),
            str(item.get("first_public_at", "")),
            str(item.get("event_id", "")),
        ),
    )
    X = build_feature_matrix(sorted_details)
    baseline_scores = [rules.score_event_detail(detail) for detail in sorted_details]
    rule_values = np.asarray([score.rule_score for score in baseline_scores], dtype=float)

    isolation_forest = IsolationForest(
        n_estimators=200,
        contamination=min(max(contamination, 0.01), 0.4),
        random_state=42,
        n_jobs=1,
    )
    isolation_forest.fit(X)
    anomaly_raw = -isolation_forest.score_samples(X)
    anomaly_norm, anomaly_bounds = _normalize_with_bounds(anomaly_raw)

    weak_signal = 0.65 * rule_values + 0.35 * anomaly_norm
    relevance = _weak_relevance_labels(weak_signal)
    ranker_model = None
    ranker_bounds: tuple[float, float] | None = None
    ranker_status = "disabled"
    ranker_reason = ""
    ranker_training_source = "none"
    reviewed_label_count = 0
    reviewed_positive_labels = 0
    reviewed_control_labels = 0

    if enable_ranker:
        try:
            LGBMRanker = _require_lightgbm_ranker()
        except RuntimeError as exc:
            ranker_status = "unavailable"
            ranker_reason = str(exc)
        else:
            reviewed_ranker_data = _reviewed_ranker_training_data(sorted_details)
            if reviewed_ranker_data is not None:
                ranker_X, ranker_y = reviewed_ranker_data
                reviewed_label_count = int(len(ranker_y))
                reviewed_positive_labels = int(sum(1 for value in ranker_y if int(value) > 0))
                reviewed_control_labels = int(sum(1 for value in ranker_y if int(value) <= 0))
                ranker_training_source = "reviewed_labels"
            else:
                ranker_X = X
                ranker_y = relevance
                ranker_training_source = "weak_labels"

            if len(set(int(label) for label in ranker_y.tolist())) < 2:
                ranker_status = "skipped"
                ranker_reason = (
                    "Ranker labels collapsed to a single class after applying reviewed-label coverage and "
                    "fallback logic."
                )
            else:
                ranker_model = LGBMRanker(
                    objective="lambdarank",
                    n_estimators=120,
                    learning_rate=0.05,
                    num_leaves=15,
                    min_child_samples=2,
                    random_state=42,
                    n_jobs=1,
                    verbosity=-1,
                )
                ranker_model.fit(ranker_X, ranker_y, group=[len(ranker_y)])
                ranker_raw = ranker_model.booster_.predict(ranker_X)
                _, ranker_bounds = _normalize_with_bounds(ranker_raw)
                ranker_status = "trained"
    else:
        ranker_reason = "Ranker disabled by configuration."

    output_dir.mkdir(parents=True, exist_ok=True)
    bundle = {
        "feature_names": _feature_names(),
        "isolation_forest": isolation_forest,
        "anomaly_bounds": anomaly_bounds,
        "ranker_model": ranker_model,
        "ranker_bounds": ranker_bounds,
    }
    bundle_path = output_dir / MODEL_BUNDLE_FILENAME
    with bundle_path.open("wb") as handle:
        pickle.dump(bundle, handle)

    manifest = {
        "trained_at": _utc_now_iso(),
        "samples": len(sorted_details),
        "feature_count": len(_feature_names()),
        "feature_names": _feature_names(),
        "contamination": contamination,
        "baseline": "rules",
        "ranker_status": ranker_status,
        "ranker_reason": ranker_reason,
        "ranker_enabled": ranker_model is not None,
        "ranker_training_source": ranker_training_source,
        "reviewed_label_count": reviewed_label_count,
        "reviewed_positive_labels": reviewed_positive_labels,
        "reviewed_control_labels": reviewed_control_labels,
        "model_bundle_path": str(bundle_path),
    }
    manifest_path = output_dir / MODEL_MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return TrainingArtifacts(
        output_dir=output_dir,
        manifest_path=manifest_path,
        bundle_path=bundle_path,
        manifest=manifest,
    )


def load_model_stack(model_dir: Path) -> dict[str, object] | None:
    bundle_path = model_dir / MODEL_BUNDLE_FILENAME
    if not bundle_path.exists():
        return None
    with bundle_path.open("rb") as handle:
        return pickle.load(handle)


def load_model_manifest(model_dir: Path) -> dict[str, object] | None:
    manifest_path = model_dir / MODEL_MANIFEST_FILENAME
    if not manifest_path.exists():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def score_event_details(
    details: list[dict[str, object]],
    *,
    engine: str,
    model_dir: Path | None = None,
) -> tuple[list[Any], dict[str, object]]:
    normalized_engine = engine.strip().lower()
    if normalized_engine not in {"auto", "rules", "hybrid"}:
        raise ValueError(f"Unsupported scoring engine: {engine}")

    model_bundle = load_model_stack(model_dir) if model_dir is not None else None
    model_manifest = load_model_manifest(model_dir) if model_dir is not None else None
    use_hybrid = normalized_engine != "rules" and model_bundle is not None

    scores = []
    fallback_reason = None
    if normalized_engine != "rules" and model_bundle is None:
        fallback_reason = f"No trained model bundle found at {model_dir}."

    for detail in details:
        baseline_score = rules.score_event_detail(detail)
        if use_hybrid:
            scores.append(_score_with_models(detail, baseline_score, model_bundle, model_manifest))
        else:
            scores.append(_score_with_rule_fallback(baseline_score, normalized_engine, fallback_reason))

    metadata = {
        "engine_requested": normalized_engine,
        "engine_used": "hybrid" if use_hybrid else "rules",
        "model_dir": str(model_dir) if model_dir is not None else None,
        "model_manifest": model_manifest,
        "fallback_reason": fallback_reason,
    }
    return scores, metadata


def _score_with_models(
    detail: dict[str, object],
    baseline_score,
    model_bundle: dict[str, object],
    model_manifest: dict[str, object] | None,
):
    np = _require_numpy()
    X = build_feature_matrix([detail])
    anomaly_raw = -model_bundle["isolation_forest"].score_samples(X)
    anomaly_score = _normalize_value(float(anomaly_raw[0]), model_bundle["anomaly_bounds"])

    ranker_model = model_bundle.get("ranker_model")
    ranker_score = None
    if ranker_model is not None and model_bundle.get("ranker_bounds") is not None:
        ranker_raw = float(ranker_model.booster_.predict(X)[0])
        ranker_score = _normalize_value(ranker_raw, model_bundle["ranker_bounds"])

    final_score = (
        0.35 * baseline_score.rule_score
        + 0.35 * anomaly_score
        + 0.30 * (ranker_score if ranker_score is not None else anomaly_score)
    )
    suspiciousness_score = round(final_score * 100, 2)
    payload = json.loads(baseline_score.explanation_payload)
    payload["summary"] = _hybrid_summary(
        suspiciousness_score=suspiciousness_score,
        rule_score=baseline_score.rule_score,
        anomaly_score=anomaly_score,
        ranker_score=ranker_score,
        manifest=model_manifest,
        baseline_summary=str(payload.get("summary", "")),
    )
    payload["model_stack"] = {
        "engine": "hybrid",
        "baseline": "rules",
        "anomaly_score": round(anomaly_score, 4),
        "ranker_score": round(ranker_score, 4) if ranker_score is not None else None,
        "final_score": round(final_score, 4),
        "model_trained_at": model_manifest.get("trained_at") if model_manifest else None,
        "ranker_status": model_manifest.get("ranker_status") if model_manifest else None,
    }
    payload.setdefault("components", {})
    payload["components"]["anomaly_score"] = round(anomaly_score, 4)
    if ranker_score is not None:
        payload["components"]["ranker_score"] = round(ranker_score, 4)
    payload["components"]["hybrid_score"] = round(final_score, 4)

    return baseline_score.__class__(
        event_id=baseline_score.event_id,
        rule_score=baseline_score.rule_score,
        suspiciousness_score=suspiciousness_score,
        score_band=_score_band(suspiciousness_score),
        directional_alignment=baseline_score.directional_alignment,
        explanation_payload=json.dumps(payload, sort_keys=True),
        scored_at=_utc_now_iso(),
    )


def _score_with_rule_fallback(baseline_score, requested_engine: str, fallback_reason: str | None):
    payload = json.loads(baseline_score.explanation_payload)
    payload["model_stack"] = {
        "engine": "rules",
        "baseline": "rules",
        "fallback_reason": fallback_reason,
        "requested_engine": requested_engine,
    }
    if requested_engine == "rules":
        payload["summary"] = f"Scored with the rule baseline only. {payload.get('summary', '')}".strip()
    elif fallback_reason:
        payload["summary"] = (
            f"Fell back to the rule baseline because {fallback_reason} {payload.get('summary', '')}"
        ).strip()

    return baseline_score.__class__(
        event_id=baseline_score.event_id,
        rule_score=baseline_score.rule_score,
        suspiciousness_score=baseline_score.suspiciousness_score,
        score_band=baseline_score.score_band,
        directional_alignment=baseline_score.directional_alignment,
        explanation_payload=json.dumps(payload, sort_keys=True),
        scored_at=_utc_now_iso(),
    )


def _feature_names() -> list[str]:
    return [
        *NUMERIC_FEATURES,
        "sentiment_direction",
        "timestamp_confidence_score",
        "official_source_flag",
        "source_is_press_release",
        "source_is_sec_filing",
        "event_type_hash",
    ]


def feature_names() -> list[str]:
    return _feature_names()


def feature_map(detail: dict[str, object]) -> dict[str, float]:
    mapped = {feature_name: _coerce_float(detail.get(feature_name)) for feature_name in NUMERIC_FEATURES}
    mapped.update(
        {
            "sentiment_direction": _sentiment_direction(detail.get("sentiment_label")),
            "timestamp_confidence_score": _timestamp_confidence(detail.get("timestamp_confidence")),
            "official_source_flag": 1.0 if bool(detail.get("official_source_flag", False)) else 0.0,
            "source_is_press_release": 1.0
            if str(detail.get("source_table", "")).strip() == "raw_issuer_releases"
            else 0.0,
            "source_is_sec_filing": 1.0
            if str(detail.get("source_table", "")).strip() == "raw_filings"
            else 0.0,
            "event_type_hash": _event_type_hash(detail.get("event_type")),
        }
    )
    return mapped


def build_feature_vector(
    detail: dict[str, object],
    *,
    feature_subset: list[str] | None = None,
) -> list[float]:
    selected_features = feature_subset or _feature_names()
    mapped = feature_map(detail)
    return [mapped.get(feature_name, 0.0) for feature_name in selected_features]


def build_feature_matrix(
    details: list[dict[str, object]],
    *,
    feature_subset: list[str] | None = None,
):
    np = _require_numpy()
    return np.asarray(
        [build_feature_vector(detail, feature_subset=feature_subset) for detail in details],
        dtype=float,
    )


def _weak_relevance_labels(values) -> Any:
    np = _require_numpy()
    if values.size == 0:
        return np.asarray([], dtype=int)
    quantiles = np.quantile(values, [0.5, 0.75, 0.9])
    return np.digitize(values, bins=quantiles, right=False).astype(int)


def _reviewed_ranker_training_data(details: list[dict[str, object]]) -> tuple[Any, Any] | None:
    labeled_details = [
        detail
        for detail in details
        if str(detail.get("benchmark_label", "")).strip().lower()
        in {POSITIVE_BENCHMARK_LABEL, CONTROL_BENCHMARK_LABEL}
    ]
    if len(labeled_details) < 6:
        return None

    positive_count = sum(
        1
        for detail in labeled_details
        if str(detail.get("benchmark_label", "")).strip().lower() == POSITIVE_BENCHMARK_LABEL
    )
    control_count = len(labeled_details) - positive_count
    if positive_count < 2 or control_count < 2:
        return None

    np = _require_numpy()
    X = build_feature_matrix(labeled_details)
    y = np.asarray(
        [
            3 if str(detail.get("benchmark_label", "")).strip().lower() == POSITIVE_BENCHMARK_LABEL else 0
            for detail in labeled_details
        ],
        dtype=int,
    )
    return X, y


def _normalize_with_bounds(values) -> tuple[Any, tuple[float, float]]:
    np = _require_numpy()
    min_value = float(np.min(values))
    max_value = float(np.max(values))
    if max_value <= min_value:
        return np.zeros_like(values, dtype=float), (min_value, max_value)
    return (values - min_value) / (max_value - min_value), (min_value, max_value)


def _normalize_value(value: float, bounds: tuple[float, float]) -> float:
    min_value, max_value = bounds
    if max_value <= min_value:
        return 0.0
    return max(0.0, min(1.0, (value - min_value) / (max_value - min_value)))


def _sentiment_direction(value: object) -> float:
    label = str(value or "neutral").strip().lower()
    if label == "positive":
        return 1.0
    if label == "negative":
        return -1.0
    return 0.0


def _timestamp_confidence(value: object) -> float:
    label = str(value or "low").strip().lower()
    mapping = {"high": 1.0, "medium": 0.6, "low": 0.2}
    return mapping.get(label, 0.2)


def _event_type_hash(value: object) -> float:
    text = str(value or "").strip().lower()
    if not text:
        return 0.0
    return (sum(ord(character) for character in text) % 97) / 97.0


def _coerce_float(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _score_band(value: float) -> str:
    if value >= 75:
        return "High"
    if value >= 45:
        return "Medium"
    return "Low"


def _hybrid_summary(
    *,
    suspiciousness_score: float,
    rule_score: float,
    anomaly_score: float,
    ranker_score: float | None,
    manifest: dict[str, object] | None,
    baseline_summary: str,
) -> str:
    pieces = [
        f"Hybrid anomaly stack scored this event at {suspiciousness_score:.2f}",
        f"rule baseline contributed {rule_score:.2f}",
        f"isolation forest anomaly score was {anomaly_score:.2f}",
    ]
    if ranker_score is not None:
        pieces.append(f"LightGBM ranker score was {ranker_score:.2f}")
    if manifest and manifest.get("trained_at"):
        pieces.append(f"models were trained at {manifest['trained_at']}")
    if baseline_summary:
        pieces.append(baseline_summary)
    return ". ".join(pieces) + "."


def _require_numpy():
    try:
        import numpy
    except ImportError as exc:
        raise RuntimeError(
            "numpy is required for the anomaly stack. Install it with `pip install -e .[ml]`."
        ) from exc
    return numpy


def _require_isolation_forest():
    try:
        from sklearn.ensemble import IsolationForest
    except ImportError as exc:
        raise RuntimeError(
            "scikit-learn is required for IsolationForest training. Install it with `pip install -e .[ml]`."
        ) from exc
    return IsolationForest


def _require_lightgbm_ranker():
    try:
        from lightgbm import LGBMRanker
    except ImportError as exc:
        raise RuntimeError(
            "lightgbm is required for the ranker stage. Install it with `pip install -e .[ml]`."
        ) from exc
    return LGBMRanker


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
