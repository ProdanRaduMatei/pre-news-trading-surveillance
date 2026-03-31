from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from ..domain import CanonicalEvent
from ..ingest.models import RawFilingRecord


EVENT_TYPE_RULES = (
    ("mna", ("acquisition", "merger", "combination", "purchase agreement")),
    ("guidance", ("guidance", "outlook", "forecast")),
    ("earnings", ("results of operations", "earnings", "financial condition", "quarterly results")),
    ("executive_change", ("departure of directors", "appointment", "resignation", "chief executive", "officer")),
    ("financing", ("equity securities", "debt", "financing", "credit agreement", "offering")),
    ("litigation_regulatory", ("lawsuit", "litigation", "subpoena", "investigation", "delisting", "regulatory")),
    ("major_business_event", ("material definitive agreement", "partnership", "approval", "contract", "launch")),
)

POSITIVE_KEYWORDS = (
    "acquisition",
    "approval",
    "agreement",
    "partnership",
    "results of operations",
    "contract",
    "launch",
    "dividend",
)
NEGATIVE_KEYWORDS = (
    "litigation",
    "investigation",
    "delisting",
    "resignation",
    "departure",
    "impairment",
    "bankruptcy",
    "guidance cut",
    "restatement",
)

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


def build_canonical_events_from_filings(filings: list[RawFilingRecord]) -> list[CanonicalEvent]:
    filings_sorted = sorted(filings, key=lambda filing: _event_datetime(filing))
    recent_events: dict[tuple[str, str], list[datetime]] = defaultdict(list)
    built_at = _utc_now()
    events: list[CanonicalEvent] = []

    for filing in filings_sorted:
        event_dt = _event_datetime(filing)
        event_type = classify_event_type(filing.form_type, filing.primary_doc_description, filing.primary_document)
        sentiment_label, sentiment_score = infer_sentiment(
            filing.primary_doc_description,
            filing.primary_document,
            event_type,
        )
        novelty = infer_novelty(recent_events[(filing.ticker, event_type)], event_dt)
        recent_events[(filing.ticker, event_type)].append(event_dt)

        summary = (
            f"{filing.company_name} disclosed a {filing.form_type} filing categorized as {event_type}. "
            f"Primary document description: {filing.primary_doc_description or 'not provided'}."
        )

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
                event_type=event_type,
                sentiment_label=sentiment_label,
                sentiment_score=sentiment_score,
                title=filing.primary_doc_description or f"{filing.form_type} filing",
                summary=summary,
                source_url=filing.source_url,
                primary_document=filing.primary_document,
                official_source_flag=True,
                timestamp_confidence="high" if filing.accepted_at else "medium",
                source_quality=1.0,
                novelty=novelty,
                impact_score=IMPACT_SCORES.get(event_type, IMPACT_SCORES["other"]),
                built_at=built_at,
            )
        )

    return sorted(events, key=lambda event: event.first_public_at, reverse=True)


def classify_event_type(
    form_type: str,
    primary_doc_description: str | None,
    primary_document: str | None,
) -> str:
    text = " ".join(
        [
            form_type or "",
            primary_doc_description or "",
            primary_document or "",
        ]
    ).lower()

    for event_type, keywords in EVENT_TYPE_RULES:
        if any(keyword in text for keyword in keywords):
            return event_type
    if form_type.upper() == "8-K":
        return "major_business_event"
    return "other"


def infer_sentiment(
    primary_doc_description: str | None,
    primary_document: str | None,
    event_type: str,
) -> tuple[str, float]:
    text = " ".join([primary_doc_description or "", primary_document or "", event_type]).lower()

    if any(keyword in text for keyword in NEGATIVE_KEYWORDS):
        return "negative", -0.75
    if any(keyword in text for keyword in POSITIVE_KEYWORDS):
        return "positive", 0.75
    if event_type in {"earnings", "guidance", "mna"}:
        return "positive", 0.25
    return "neutral", 0.0


def infer_novelty(prior_event_times: list[datetime], event_dt: datetime) -> float:
    if not prior_event_times:
        return 1.0

    recent_count = 0
    for prior_time in prior_event_times:
        if event_dt - prior_time <= timedelta(days=30):
            recent_count += 1

    if recent_count == 0:
        return 0.9
    if recent_count == 1:
        return 0.55
    return 0.3


def _event_datetime(filing: RawFilingRecord) -> datetime:
    if filing.accepted_at:
        return datetime.fromisoformat(filing.accepted_at.replace("Z", "+00:00")).astimezone(timezone.utc)
    if filing.filing_date:
        filing_day = date.fromisoformat(filing.filing_date)
        return datetime.combine(filing_day, datetime.min.time(), tzinfo=timezone.utc)
    return datetime.fromisoformat(filing.ingested_at.replace("Z", "+00:00")).astimezone(timezone.utc)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
