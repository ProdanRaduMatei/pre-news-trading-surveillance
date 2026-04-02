from __future__ import annotations

from pathlib import Path

from .. import db
from . import anomaly_stack


def load_public_model_summary(db_path: Path, *, model_dir: Path) -> dict[str, object]:
    latest_runs = db.get_latest_successful_runs(db_path, ["train_model_stack", "score_events"])
    train_run = latest_runs.get("train_model_stack")
    score_run = latest_runs.get("score_events")
    manifest = anomaly_stack.load_model_manifest(model_dir) or _manifest_from_run(train_run)

    if not manifest and not score_run:
        return pending_public_model_summary()

    score_metadata = dict((score_run or {}).get("metadata") or {})
    scoring_metadata = dict(score_metadata.get("scoring_metadata") or {})
    engine_used = scoring_metadata.get("engine_used") or score_metadata.get("engine") or "rules"
    engine_requested = scoring_metadata.get("engine_requested") or score_metadata.get("engine") or engine_used

    status = "available" if manifest else "rules_only"
    notice = (
        "The latest successful scoring run used the trained anomaly stack."
        if engine_used == "hybrid"
        else "The latest successful scoring run used the rule baseline or fell back to it."
    )
    if manifest and manifest.get("ranker_training_source") == "reviewed_labels":
        notice += " The LightGBM ranker was trained on reviewed suspicious/control benchmark labels."
    elif manifest and manifest.get("ranker_status") == "trained":
        notice += " The LightGBM ranker is currently trained on weak supervision from anomaly and rule signals."

    return {
        "status": status,
        "engine_used": engine_used,
        "engine_requested": engine_requested,
        "trained_at": manifest.get("trained_at") if manifest else None,
        "training_samples": _optional_int(manifest.get("samples")) if manifest else None,
        "feature_count": _optional_int(manifest.get("feature_count")) if manifest else None,
        "ranker_enabled": bool(manifest.get("ranker_enabled")) if manifest else False,
        "ranker_status": manifest.get("ranker_status") if manifest else None,
        "ranker_reason": manifest.get("ranker_reason") if manifest else None,
        "ranker_training_source": manifest.get("ranker_training_source") if manifest else None,
        "reviewed_label_count": _optional_int(manifest.get("reviewed_label_count")) if manifest else None,
        "reviewed_positive_labels": _optional_int(manifest.get("reviewed_positive_labels"))
        if manifest
        else None,
        "reviewed_control_labels": _optional_int(manifest.get("reviewed_control_labels"))
        if manifest
        else None,
        "score_run": {
            "run_id": (score_run or {}).get("run_id"),
            "finished_at": (score_run or {}).get("finished_at"),
            "started_at": (score_run or {}).get("started_at"),
        },
        "train_run": {
            "run_id": (train_run or {}).get("run_id"),
            "finished_at": (train_run or {}).get("finished_at"),
            "started_at": (train_run or {}).get("started_at"),
        },
        "notice": notice,
    }


def pending_public_model_summary() -> dict[str, object]:
    return {
        "status": "pending",
        "engine_used": None,
        "engine_requested": None,
        "trained_at": None,
        "training_samples": None,
        "feature_count": None,
        "ranker_enabled": False,
        "ranker_status": None,
        "ranker_reason": None,
        "ranker_training_source": None,
        "reviewed_label_count": None,
        "reviewed_positive_labels": None,
        "reviewed_control_labels": None,
        "score_run": {},
        "train_run": {},
        "notice": (
            "No trained model manifest has been published yet. The system can still score with the rule "
            "baseline until a training run succeeds."
        ),
    }


def _manifest_from_run(run: dict[str, object] | None) -> dict[str, object] | None:
    if not run:
        return None
    metadata = dict(run.get("metadata") or {})
    manifest = metadata.get("model_manifest")
    return dict(manifest) if isinstance(manifest, dict) else None


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
