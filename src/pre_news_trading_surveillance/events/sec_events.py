from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from ..domain import CanonicalEvent
from ..ingest.models import RawFilingRecord
from ..nlp.novelty import build_novelty_backend
from ..nlp.sec_taxonomy import build_event_text, classify_event_type, parse_sec_items
from ..nlp.sentiment import build_sentiment_backend


IMPACT_SCORES = {
    "mna": 0.95,
    "guidance": 0.9,
    "earnings": 0.85,
    "litigation_regulatory": 0.8,
    "financing": 0.75,
    "major_business_event": 0.7,
    "executive_change": 0.6,
    "other": 0.5,
}


def build_canonical_events_from_filings(
    filings: list[RawFilingRecord],
    sentiment_backend_name: str = "heuristic",
    novelty_backend_name: str = "lexical",
    sentiment_model: str | None = None,
    novelty_model: str | None = None,
) -> list[CanonicalEvent]:
    filings_sorted = sorted(filings, key=lambda filing: _event_datetime(filing))
    recent_events: dict[str, list[tuple[datetime, str]]] = defaultdict(list)
    built_at = _utc_now()
    events: list[CanonicalEvent] = []
    sentiment_backend = build_sentiment_backend(sentiment_backend_name, sentiment_model)
    novelty_backend = build_novelty_backend(novelty_backend_name, novelty_model)

    for filing in filings_sorted:
        event_dt = _event_datetime(filing)
        sec_items = parse_sec_items(filing.items_json)
        event_type_result = classify_event_type(
            filing.form_type,
            filing.primary_doc_description,
            filing.primary_document,
            sec_items,
        )
        sentiment_result = sentiment_backend.analyze(
            filing.form_type,
            filing.primary_doc_description,
            filing.primary_document,
            event_type_result.label,
            filing.items_json,
        )
        event_text = build_event_text(
            filing.form_type,
            filing.primary_doc_description,
            filing.primary_document,
        )
        novelty_history = [
            prior_text
            for prior_dt, prior_text in recent_events[filing.ticker]
            if event_dt - prior_dt <= timedelta(days=30)
        ]
        novelty_result = novelty_backend.score(event_text, novelty_history)
        recent_events[filing.ticker].append((event_dt, event_text))

        summary = (
            f"{filing.company_name} disclosed a {filing.form_type} filing categorized as "
            f"{event_type_result.label} using {event_type_result.backend}. "
            f"Primary document description: {filing.primary_doc_description or 'not provided'}."
        )
        if sec_items:
            summary += f" SEC items: {', '.join(sec_items)}."

        first_public_at = event_dt.replace(tzinfo=timezone.utc).isoformat()
        events.append(
            CanonicalEvent(
                event_id=f"sec-event:{filing.filing_id}",
                source_event_id=filing.filing_id,
                source_table="raw_filings",
                ticker=filing.ticker.upper(),
                issuer_name=filing.company_name,
                first_public_at=first_public_at,
                event_date=event_dt.date().isoformat(),
                event_type=event_type_result.label,
                sentiment_label=sentiment_result.label,
                sentiment_score=sentiment_result.score,
                title=filing.primary_doc_description or f"{filing.form_type} filing",
                summary=summary,
                source_url=filing.source_url,
                primary_document=filing.primary_document,
                sec_items_json=json.dumps(sec_items) if sec_items else None,
                official_source_flag=True,
                timestamp_confidence="high" if filing.accepted_at else "medium",
                classifier_backend=event_type_result.backend,
                sentiment_backend=sentiment_result.backend,
                novelty_backend=novelty_result.backend,
                source_quality=1.0,
                novelty=novelty_result.score,
                impact_score=IMPACT_SCORES.get(event_type_result.label, IMPACT_SCORES["other"]),
                built_at=built_at,
            )
        )

    return sorted(events, key=lambda event: event.first_public_at, reverse=True)


def _event_datetime(filing: RawFilingRecord) -> datetime:
    if filing.accepted_at:
        return datetime.fromisoformat(filing.accepted_at.replace("Z", "+00:00")).astimezone(timezone.utc)
    if filing.filing_date:
        filing_day = date.fromisoformat(filing.filing_date)
        return datetime.combine(filing_day, datetime.min.time(), tzinfo=timezone.utc)
    return datetime.fromisoformat(filing.ingested_at.replace("Z", "+00:00")).astimezone(timezone.utc)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
