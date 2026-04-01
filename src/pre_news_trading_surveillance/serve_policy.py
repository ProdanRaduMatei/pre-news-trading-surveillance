from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os

RESEARCH_NOTICE = (
    "This product is a public-data research and triage tool. It does not identify traders, "
    "assert intent, or constitute evidence of illegal insider trading."
)
LIMITATION_NOTICE = (
    "Scores reflect unusual pre-disclosure market behavior around official issuer events. "
    "They are designed for review prioritization, not accusation or enforcement."
)


@dataclass(frozen=True)
class ServePolicy:
    public_safe_mode: bool = False
    delay_minutes: int = 0
    data_source_mode: str = "duckdb"

    def cutoff_at(self, *, now: datetime | None = None) -> datetime | None:
        if not self.public_safe_mode:
            return None
        effective_delay = self.delay_minutes if self.delay_minutes > 0 else 1440
        reference = now or datetime.now(timezone.utc)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        return reference.astimezone(timezone.utc) - timedelta(minutes=effective_delay)

    def cutoff_at_iso(self, *, now: datetime | None = None) -> str | None:
        cutoff = self.cutoff_at(now=now)
        if cutoff is None:
            return None
        return cutoff.replace(microsecond=0).isoformat()

    def is_visible(self, first_public_at: str | None, *, now: datetime | None = None) -> bool:
        if not self.public_safe_mode:
            return True
        if not first_public_at:
            return False
        published_at = parse_datetime(first_public_at)
        cutoff = self.cutoff_at(now=now)
        if published_at is None or cutoff is None:
            return False
        return published_at <= cutoff

    def metadata(self, *, now: datetime | None = None) -> dict[str, object]:
        effective_delay = self.delay_minutes if self.public_safe_mode and self.delay_minutes > 0 else (
            1440 if self.public_safe_mode else 0
        )
        return {
            "public_safe_mode": self.public_safe_mode,
            "delay_minutes": effective_delay,
            "delay_label": _format_delay_label(effective_delay),
            "cutoff_at": self.cutoff_at_iso(now=now),
            "data_source_mode": self.data_source_mode,
            "research_notice": RESEARCH_NOTICE,
            "limitation_notice": LIMITATION_NOTICE,
        }


def policy_from_env(*, data_source_mode: str = "duckdb") -> ServePolicy:
    public_safe_mode = _env_flag("PNTS_PUBLIC_SAFE_MODE", default=False)
    raw_delay = os.getenv("PNTS_PUBLIC_DELAY_MINUTES", "").strip()
    delay_minutes = int(raw_delay) if raw_delay else (1440 if public_safe_mode else 0)
    return ServePolicy(
        public_safe_mode=public_safe_mode,
        delay_minutes=max(delay_minutes, 0),
        data_source_mode=data_source_mode,
    )


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _env_flag(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _format_delay_label(delay_minutes: int) -> str:
    if delay_minutes <= 0:
        return "No delay"
    if delay_minutes % 1440 == 0:
        days = delay_minutes // 1440
        return f"{days} day" if days == 1 else f"{days} days"
    if delay_minutes % 60 == 0:
        hours = delay_minutes // 60
        return f"{hours} hour" if hours == 1 else f"{hours} hours"
    return f"{delay_minutes} minutes"
