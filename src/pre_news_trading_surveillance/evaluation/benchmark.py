from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from ..domain import BenchmarkLabel
from ..scoring import rules

VALID_BENCHMARK_LABELS = {"suspicious", "control", "unknown"}
VALID_REVIEW_STATUSES = {"pending_review", "reviewed", "rejected"}


def export_review_candidates(
    details: list[dict[str, object]],
    *,
    output_path: Path,
    top_k: int = 50,
    bottom_k: int = 50,
) -> int:
    rows = build_review_candidate_rows(details, top_k=top_k, bottom_k=bottom_k)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_candidate_fieldnames())
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def build_review_candidate_rows(
    details: list[dict[str, object]],
    *,
    top_k: int = 50,
    bottom_k: int = 50,
) -> list[dict[str, object]]:
    ranked = sorted(
        [_candidate_row(detail) for detail in details],
        key=lambda item: (
            -float(item["current_score"]),
            str(item["first_public_at"]),
            str(item["event_id"]),
        ),
    )
    selected: list[dict[str, object]] = []
    seen_event_ids: set[str] = set()

    for bucket_name, candidates in (
        ("high_priority", ranked[:top_k]),
        ("control_candidate", list(reversed(ranked[-bottom_k:]))),
    ):
        for candidate in candidates:
            event_id = str(candidate["event_id"])
            if event_id in seen_event_ids:
                continue
            seen_event_ids.add(event_id)
            candidate["candidate_bucket"] = bucket_name
            candidate["suggested_label"] = "suspicious" if bucket_name == "high_priority" else "control"
            candidate["review_label"] = ""
            candidate["review_status"] = "pending_review"
            candidate["reviewer"] = ""
            candidate["confidence"] = ""
            candidate["review_notes"] = ""
            selected.append(candidate)

    return selected


def load_reviewed_labels_csv(
    csv_path: Path,
    *,
    default_reviewer: str | None = None,
    default_label_source: str = "manual_review",
) -> list[BenchmarkLabel]:
    labels: list[BenchmarkLabel] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            event_id = str(row.get("event_id", "")).strip()
            raw_label = str(row.get("review_label") or row.get("benchmark_label") or "").strip().lower()
            if not event_id or not raw_label:
                continue
            if raw_label not in VALID_BENCHMARK_LABELS:
                raise ValueError(
                    f"Unsupported benchmark label '{raw_label}' in {csv_path}. Expected one of {sorted(VALID_BENCHMARK_LABELS)}."
                )

            review_status = str(row.get("review_status") or "reviewed").strip().lower()
            if review_status not in VALID_REVIEW_STATUSES:
                raise ValueError(
                    f"Unsupported review status '{review_status}' in {csv_path}. Expected one of {sorted(VALID_REVIEW_STATUSES)}."
                )

            reviewer = str(row.get("reviewer") or default_reviewer or "").strip() or None
            label_source = str(row.get("label_source") or default_label_source).strip() or default_label_source
            confidence = _optional_float(row.get("confidence"))
            review_notes = str(row.get("review_notes") or "").strip() or None
            metadata = {
                "candidate_bucket": row.get("candidate_bucket") or None,
                "suggested_label": row.get("suggested_label") or None,
                "title": row.get("title") or None,
                "source_url": row.get("source_url") or None,
            }
            metadata_json = json.dumps(
                {key: value for key, value in metadata.items() if value is not None},
                sort_keys=True,
            )
            timestamp = _utc_now_iso()
            labels.append(
                BenchmarkLabel(
                    event_id=event_id,
                    benchmark_label=raw_label,
                    review_status=review_status,
                    reviewer=reviewer,
                    label_source=label_source,
                    confidence=confidence,
                    review_notes=review_notes,
                    metadata_json=metadata_json if metadata_json != "{}" else None,
                    created_at=timestamp,
                    updated_at=timestamp,
                )
            )
    return labels


def _candidate_row(detail: dict[str, object]) -> dict[str, object]:
    score_value = detail.get("suspiciousness_score")
    score_band = detail.get("score_band")
    if score_value is None or score_band is None:
        baseline = rules.score_event_detail(detail)
        score_value = baseline.suspiciousness_score
        score_band = baseline.score_band

    return {
        "event_id": detail.get("event_id", ""),
        "ticker": detail.get("ticker", ""),
        "issuer_name": detail.get("issuer_name", ""),
        "first_public_at": detail.get("first_public_at", ""),
        "event_type": detail.get("event_type", ""),
        "title": detail.get("title", ""),
        "current_score": round(float(score_value or 0.0), 2),
        "score_band": score_band or "",
        "source_url": detail.get("source_url", ""),
    }


def _candidate_fieldnames() -> list[str]:
    return [
        "event_id",
        "ticker",
        "issuer_name",
        "first_public_at",
        "event_type",
        "title",
        "current_score",
        "score_band",
        "source_url",
        "candidate_bucket",
        "suggested_label",
        "review_label",
        "review_status",
        "reviewer",
        "confidence",
        "review_notes",
    ]


def _optional_float(value: object) -> float | None:
    if value in {None, ""}:
        return None
    return float(value)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
