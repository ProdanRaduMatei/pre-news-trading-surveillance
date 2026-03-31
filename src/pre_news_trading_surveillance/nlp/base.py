from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class EventTypeResult:
    label: str
    confidence: float
    backend: str
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SentimentResult:
    label: str
    score: float
    confidence: float
    backend: str
    raw_label: str | None = None


@dataclass(frozen=True)
class NoveltyResult:
    score: float
    backend: str
    confidence: float
    max_similarity: float
    compared_items: int
