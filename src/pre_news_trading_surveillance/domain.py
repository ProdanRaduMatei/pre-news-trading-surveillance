from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class MarketBarDaily:
    bar_id: str
    ticker: str
    trading_date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str
    ingested_at: str

    def as_db_row(self) -> tuple[str, str, str, float, float, float, float, int, str, str]:
        return (
            self.bar_id,
            self.ticker,
            self.trading_date,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.source,
            self.ingested_at,
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MarketBarMinute:
    bar_id: str
    ticker: str
    bar_start: str
    trading_date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str
    ingested_at: str

    def as_db_row(
        self,
    ) -> tuple[str, str, str, str, float, float, float, float, int, str, str]:
        return (
            self.bar_id,
            self.ticker,
            self.bar_start,
            self.trading_date,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.source,
            self.ingested_at,
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class CanonicalEvent:
    event_id: str
    source_event_id: str
    source_table: str
    ticker: str
    issuer_name: str
    first_public_at: str
    event_date: str
    event_type: str
    sentiment_label: str
    sentiment_score: float
    title: str
    summary: str
    source_url: str
    primary_document: str | None
    sec_items_json: str | None
    official_source_flag: bool
    timestamp_confidence: str
    classifier_backend: str
    sentiment_backend: str
    novelty_backend: str
    source_quality: float
    novelty: float
    impact_score: float
    built_at: str

    def as_db_row(
        self,
    ) -> tuple[
        str,
        str,
        str,
        str,
        str,
        str,
        str,
        str,
        str,
        float,
        str,
        str,
        str,
        str | None,
        str | None,
        bool,
        str,
        str,
        str,
        str,
        float,
        float,
        float,
        str,
    ]:
        return (
            self.event_id,
            self.source_event_id,
            self.source_table,
            self.ticker,
            self.issuer_name,
            self.first_public_at,
            self.event_date,
            self.event_type,
            self.sentiment_label,
            self.sentiment_score,
            self.title,
            self.summary,
            self.source_url,
            self.primary_document,
            self.sec_items_json,
            self.official_source_flag,
            self.timestamp_confidence,
            self.classifier_backend,
            self.sentiment_backend,
            self.novelty_backend,
            self.source_quality,
            self.novelty,
            self.impact_score,
            self.built_at,
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class EventMarketFeature:
    event_id: str
    ticker: str
    as_of_date: str
    pre_1d_return: float | None
    pre_5d_return: float | None
    pre_20d_return: float | None
    volume_z_1d: float | None
    volume_z_5d: float | None
    volatility_20d: float | None
    gap_pct: float | None
    avg_volume_20d: float | None
    bars_used: int
    computed_at: str

    def as_db_row(
        self,
    ) -> tuple[
        str,
        str,
        str,
        float | None,
        float | None,
        float | None,
        float | None,
        float | None,
        float | None,
        float | None,
        float | None,
        int,
        str,
    ]:
        return (
            self.event_id,
            self.ticker,
            self.as_of_date,
            self.pre_1d_return,
            self.pre_5d_return,
            self.pre_20d_return,
            self.volume_z_1d,
            self.volume_z_5d,
            self.volatility_20d,
            self.gap_pct,
            self.avg_volume_20d,
            self.bars_used,
            self.computed_at,
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class EventMarketFeatureMinute:
    event_id: str
    ticker: str
    as_of_timestamp: str
    pre_15m_return: float | None
    pre_60m_return: float | None
    pre_240m_return: float | None
    volume_z_15m: float | None
    volume_z_60m: float | None
    realized_vol_60m: float | None
    range_pct_60m: float | None
    last_bar_at: str | None
    bars_used: int
    computed_at: str

    def as_db_row(
        self,
    ) -> tuple[
        str,
        str,
        str,
        float | None,
        float | None,
        float | None,
        float | None,
        float | None,
        float | None,
        float | None,
        str | None,
        int,
        str,
    ]:
        return (
            self.event_id,
            self.ticker,
            self.as_of_timestamp,
            self.pre_15m_return,
            self.pre_60m_return,
            self.pre_240m_return,
            self.volume_z_15m,
            self.volume_z_60m,
            self.realized_vol_60m,
            self.range_pct_60m,
            self.last_bar_at,
            self.bars_used,
            self.computed_at,
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class EventScore:
    event_id: str
    rule_score: float
    suspiciousness_score: float
    score_band: str
    directional_alignment: bool
    explanation_payload: str
    scored_at: str

    def as_db_row(self) -> tuple[str, float, float, str, bool, str, str]:
        return (
            self.event_id,
            self.rule_score,
            self.suspiciousness_score,
            self.score_band,
            self.directional_alignment,
            self.explanation_payload,
            self.scored_at,
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class BenchmarkLabel:
    event_id: str
    benchmark_label: str
    review_status: str
    reviewer: str | None
    label_source: str
    confidence: float | None
    review_notes: str | None
    metadata_json: str | None
    created_at: str
    updated_at: str

    def as_db_row(
        self,
    ) -> tuple[
        str,
        str,
        str,
        str | None,
        str,
        float | None,
        str | None,
        str | None,
        str,
        str,
    ]:
        return (
            self.event_id,
            self.benchmark_label,
            self.review_status,
            self.reviewer,
            self.label_source,
            self.confidence,
            self.review_notes,
            self.metadata_json,
            self.created_at,
            self.updated_at,
        )

    def as_dict(self) -> dict[str, object]:
        return asdict(self)
