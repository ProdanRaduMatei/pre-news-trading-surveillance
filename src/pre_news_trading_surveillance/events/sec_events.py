from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from ..domain import CanonicalEvent
from ..ingest.models import RawFilingRecord, RawIssuerReleaseRecord
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


@dataclass(frozen=True)
class _DraftOfficialEvent:
    event_id: str
    source_event_id: str
    source_table: str
    ticker: str
    issuer_name: str
    occurred_at: datetime
    event_date: str
    event_type: str
    classifier_backend: str
    sentiment_label: str
    sentiment_score: float
    sentiment_backend: str
    title: str
    summary: str
    source_url: str
    primary_document: str | None
    sec_items_json: str | None
    official_source_flag: bool
    timestamp_confidence: str
    source_quality: float
    impact_score: float
    novelty_text: str


def build_canonical_events_from_filings(
    filings: list[RawFilingRecord],
    sentiment_backend_name: str = "heuristic",
    novelty_backend_name: str = "lexical",
    sentiment_model: str | None = None,
    novelty_model: str | None = None,
) -> list[CanonicalEvent]:
    return build_canonical_events_from_sources(
        filings=filings,
        issuer_releases=[],
        sentiment_backend_name=sentiment_backend_name,
        novelty_backend_name=novelty_backend_name,
        sentiment_model=sentiment_model,
        novelty_model=novelty_model,
    )


def build_canonical_events_from_press_releases(
    issuer_releases: list[RawIssuerReleaseRecord],
    sentiment_backend_name: str = "heuristic",
    novelty_backend_name: str = "lexical",
    sentiment_model: str | None = None,
    novelty_model: str | None = None,
) -> list[CanonicalEvent]:
    return build_canonical_events_from_sources(
        filings=[],
        issuer_releases=issuer_releases,
        sentiment_backend_name=sentiment_backend_name,
        novelty_backend_name=novelty_backend_name,
        sentiment_model=sentiment_model,
        novelty_model=novelty_model,
    )


def build_canonical_events_from_sources(
    *,
    filings: list[RawFilingRecord],
    issuer_releases: list[RawIssuerReleaseRecord],
    sentiment_backend_name: str = "heuristic",
    novelty_backend_name: str = "lexical",
    sentiment_model: str | None = None,
    novelty_model: str | None = None,
) -> list[CanonicalEvent]:
    drafts = _build_draft_events(
        filings=filings,
        issuer_releases=issuer_releases,
        sentiment_backend_name=sentiment_backend_name,
        sentiment_model=sentiment_model,
    )
    novelty_backend = build_novelty_backend(novelty_backend_name, novelty_model)
    recent_events: dict[str, list[tuple[datetime, str]]] = defaultdict(list)
    built_at = _utc_now()
    events: list[CanonicalEvent] = []

    for draft in sorted(drafts, key=lambda item: item.occurred_at):
        novelty_history = [
            prior_text
            for prior_dt, prior_text in recent_events[draft.ticker]
            if draft.occurred_at - prior_dt <= timedelta(days=30)
        ]
        novelty_result = novelty_backend.score(draft.novelty_text, novelty_history)
        recent_events[draft.ticker].append((draft.occurred_at, draft.novelty_text))
        events.append(
            CanonicalEvent(
                event_id=draft.event_id,
                source_event_id=draft.source_event_id,
                source_table=draft.source_table,
                ticker=draft.ticker,
                issuer_name=draft.issuer_name,
                first_public_at=draft.occurred_at.replace(tzinfo=timezone.utc).isoformat(),
                event_date=draft.event_date,
                event_type=draft.event_type,
                sentiment_label=draft.sentiment_label,
                sentiment_score=draft.sentiment_score,
                title=draft.title,
                summary=draft.summary,
                source_url=draft.source_url,
                primary_document=draft.primary_document,
                sec_items_json=draft.sec_items_json,
                official_source_flag=draft.official_source_flag,
                timestamp_confidence=draft.timestamp_confidence,
                classifier_backend=draft.classifier_backend,
                sentiment_backend=draft.sentiment_backend,
                novelty_backend=novelty_result.backend,
                source_quality=draft.source_quality,
                novelty=novelty_result.score,
                impact_score=draft.impact_score,
                built_at=built_at,
            )
        )

    return sorted(events, key=lambda event: event.first_public_at, reverse=True)


def _build_draft_events(
    *,
    filings: list[RawFilingRecord],
    issuer_releases: list[RawIssuerReleaseRecord],
    sentiment_backend_name: str,
    sentiment_model: str | None,
) -> list[_DraftOfficialEvent]:
    sentiment_backend = build_sentiment_backend(sentiment_backend_name, sentiment_model)
    drafts = [_draft_filing_event(filing, sentiment_backend) for filing in filings]
    drafts.extend(_draft_press_release_event(release, sentiment_backend) for release in issuer_releases)
    return drafts


def _draft_filing_event(filing: RawFilingRecord, sentiment_backend) -> _DraftOfficialEvent:
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
    summary = (
        f"{filing.company_name} disclosed a {filing.form_type} filing categorized as "
        f"{event_type_result.label} using {event_type_result.backend}. "
        f"Primary document description: {filing.primary_doc_description or 'not provided'}."
    )
    if sec_items:
        summary += f" SEC items: {', '.join(sec_items)}."
    return _DraftOfficialEvent(
        event_id=f"sec-event:{filing.filing_id}",
        source_event_id=filing.filing_id,
        source_table="raw_filings",
        ticker=filing.ticker.upper(),
        issuer_name=filing.company_name,
        occurred_at=event_dt,
        event_date=event_dt.date().isoformat(),
        event_type=event_type_result.label,
        classifier_backend=event_type_result.backend,
        sentiment_label=sentiment_result.label,
        sentiment_score=sentiment_result.score,
        sentiment_backend=sentiment_result.backend,
        title=filing.primary_doc_description or f"{filing.form_type} filing",
        summary=summary,
        source_url=filing.source_url,
        primary_document=filing.primary_document,
        sec_items_json=json.dumps(sec_items) if sec_items else None,
        official_source_flag=True,
        timestamp_confidence="high" if filing.accepted_at else "medium",
        source_quality=1.0,
        impact_score=IMPACT_SCORES.get(event_type_result.label, IMPACT_SCORES["other"]),
        novelty_text=build_event_text(
            filing.form_type,
            filing.primary_doc_description,
            filing.primary_document,
        ),
    )


def _draft_press_release_event(release: RawIssuerReleaseRecord, sentiment_backend) -> _DraftOfficialEvent:
    event_dt = _issuer_release_datetime(release)
    event_type_result = classify_event_type(
        "PRESS_RELEASE",
        release.title,
        release.summary_text,
        [],
    )
    sentiment_result = sentiment_backend.analyze(
        "PRESS_RELEASE",
        release.title,
        release.summary_text,
        event_type_result.label,
        None,
    )
    excerpt = release.summary_text or "No summary text was provided in the feed entry."
    summary = (
        f"{release.issuer_name} published an official {release.source_name} release categorized as "
        f"{event_type_result.label} using {event_type_result.backend}. "
        f"Headline: {release.title}. Excerpt: {excerpt}"
    )
    return _DraftOfficialEvent(
        event_id=f"issuer-release-event:{release.release_id}",
        source_event_id=release.release_id,
        source_table="raw_issuer_releases",
        ticker=release.ticker.upper(),
        issuer_name=release.issuer_name,
        occurred_at=event_dt,
        event_date=event_dt.date().isoformat(),
        event_type=event_type_result.label,
        classifier_backend=event_type_result.backend,
        sentiment_label=sentiment_result.label,
        sentiment_score=sentiment_result.score,
        sentiment_backend=sentiment_result.backend,
        title=release.title,
        summary=summary,
        source_url=release.source_url,
        primary_document=None,
        sec_items_json=None,
        official_source_flag=True,
        timestamp_confidence="high" if release.published_at else "medium",
        source_quality=0.95,
        impact_score=IMPACT_SCORES.get(event_type_result.label, IMPACT_SCORES["other"]),
        novelty_text=build_event_text("PRESS_RELEASE", release.title, release.summary_text),
    )


def _event_datetime(filing: RawFilingRecord) -> datetime:
    if filing.accepted_at:
        return datetime.fromisoformat(filing.accepted_at.replace("Z", "+00:00")).astimezone(timezone.utc)
    if filing.filing_date:
        filing_day = date.fromisoformat(filing.filing_date)
        return datetime.combine(filing_day, datetime.min.time(), tzinfo=timezone.utc)
    return datetime.fromisoformat(filing.ingested_at.replace("Z", "+00:00")).astimezone(timezone.utc)


def _issuer_release_datetime(release: RawIssuerReleaseRecord) -> datetime:
    if release.published_at:
        return datetime.fromisoformat(release.published_at.replace("Z", "+00:00")).astimezone(timezone.utc)
    return datetime.fromisoformat(release.ingested_at.replace("Z", "+00:00")).astimezone(timezone.utc)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
