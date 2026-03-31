from __future__ import annotations

import json
from datetime import datetime, timezone

from .. import db
from ..domain import CanonicalEvent, EventScore


def score_events_from_database(db_path, events: list[CanonicalEvent]) -> list[EventScore]:
    scored = []
    for event in events:
        detail = db.get_ranked_event(db_path, event.event_id)
        if detail is None:
            continue
        scored.append(score_event_detail(detail))
    return scored


def score_event_detail(detail: dict[str, object]) -> EventScore:
    sentiment_label = str(detail.get("sentiment_label", "neutral"))
    sentiment_score = _as_float(detail.get("sentiment_score")) or 0.0
    pre_1d_return = _as_float(detail.get("pre_1d_return"))
    pre_5d_return = _as_float(detail.get("pre_5d_return"))
    volume_z_1d = _as_float(detail.get("volume_z_1d"))
    volume_z_5d = _as_float(detail.get("volume_z_5d"))
    novelty = _as_float(detail.get("novelty")) or 0.0
    impact_score = _as_float(detail.get("impact_score")) or 0.0
    source_quality = _as_float(detail.get("source_quality")) or 0.0
    classifier_backend = str(detail.get("classifier_backend", "unknown"))
    sentiment_backend = str(detail.get("sentiment_backend", "unknown"))
    novelty_backend = str(detail.get("novelty_backend", "unknown"))

    directional_signal = max(pre_1d_return or 0.0, pre_5d_return or 0.0)
    if sentiment_label == "negative":
        directional_signal = max(-(pre_1d_return or 0.0), -(pre_5d_return or 0.0))
    if sentiment_label == "neutral":
        directional_signal = max(abs(pre_1d_return or 0.0), abs(pre_5d_return or 0.0)) * 0.6

    sentiment_strength = 0.25 + min(0.75, abs(sentiment_score))
    alignment_component = _clip01((directional_signal * sentiment_strength) / 0.08)
    volume_component = _clip01(max(volume_z_1d or 0.0, volume_z_5d or 0.0) / 4.0)

    rule_score = (
        0.35 * alignment_component
        + 0.25 * volume_component
        + 0.15 * impact_score
        + 0.15 * novelty
        + 0.10 * source_quality
    )
    suspiciousness_score = round(rule_score * 100, 2)
    score_band = (
        "High"
        if suspiciousness_score >= 75
        else "Medium"
        if suspiciousness_score >= 45
        else "Low"
    )

    explanation_payload = {
        "summary": _build_summary(
            sentiment_label=sentiment_label,
            pre_1d_return=pre_1d_return,
            pre_5d_return=pre_5d_return,
            volume_z_1d=volume_z_1d,
            volume_z_5d=volume_z_5d,
            impact_score=impact_score,
            novelty=novelty,
            sentiment_score=sentiment_score,
            classifier_backend=classifier_backend,
            sentiment_backend=sentiment_backend,
            novelty_backend=novelty_backend,
        ),
        "components": {
            "alignment_component": round(alignment_component, 4),
            "volume_component": round(volume_component, 4),
            "impact_score": round(impact_score, 4),
            "novelty": round(novelty, 4),
            "source_quality": round(source_quality, 4),
        },
        "signals": {
            "pre_1d_return": pre_1d_return,
            "pre_5d_return": pre_5d_return,
            "volume_z_1d": volume_z_1d,
            "volume_z_5d": volume_z_5d,
            "sentiment_label": sentiment_label,
            "sentiment_score": sentiment_score,
            "classifier_backend": classifier_backend,
            "sentiment_backend": sentiment_backend,
            "novelty_backend": novelty_backend,
        },
    }

    return EventScore(
        event_id=str(detail["event_id"]),
        rule_score=round(rule_score, 4),
        suspiciousness_score=suspiciousness_score,
        score_band=score_band,
        directional_alignment=alignment_component >= 0.35,
        explanation_payload=json.dumps(explanation_payload, sort_keys=True),
        scored_at=_utc_now(),
    )


def _build_summary(
    sentiment_label: str,
    pre_1d_return: float | None,
    pre_5d_return: float | None,
    volume_z_1d: float | None,
    volume_z_5d: float | None,
    impact_score: float,
    novelty: float,
    sentiment_score: float,
    classifier_backend: str,
    sentiment_backend: str,
    novelty_backend: str,
) -> str:
    pieces = []
    if pre_1d_return is not None:
        pieces.append(f"pre-event 1D return was {pre_1d_return:.2%}")
    if pre_5d_return is not None:
        pieces.append(f"5D drift was {pre_5d_return:.2%}")
    if volume_z_1d is not None:
        pieces.append(f"1D volume z-score was {volume_z_1d:.2f}")
    elif volume_z_5d is not None:
        pieces.append(f"5D volume z-score was {volume_z_5d:.2f}")
    pieces.append(f"event sentiment was {sentiment_label}")
    pieces.append(f"sentiment score was {sentiment_score:.2f}")
    pieces.append(f"impact prior was {impact_score:.2f}")
    pieces.append(f"novelty was {novelty:.2f}")
    pieces.append(
        f"classification used {classifier_backend}, sentiment used {sentiment_backend}, and novelty used {novelty_backend}"
    )
    return "Flagged because " + ", ".join(pieces) + "."


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
