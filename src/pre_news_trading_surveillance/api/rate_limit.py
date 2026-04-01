from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Lock
import os
import time


@dataclass(frozen=True)
class RateLimitConfig:
    enabled: bool
    max_requests: int
    window_seconds: int


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    retry_after_seconds: int
    reset_after_seconds: int


class InMemoryRateLimiter:
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._lock = Lock()
        self._requests_by_key: dict[str, deque[float]] = {}

    def check(self, key: str) -> RateLimitResult:
        if not self.config.enabled or self.config.max_requests <= 0 or self.config.window_seconds <= 0:
            return RateLimitResult(
                allowed=True,
                limit=max(self.config.max_requests, 0),
                remaining=max(self.config.max_requests, 0),
                retry_after_seconds=0,
                reset_after_seconds=0,
            )

        now = time.monotonic()
        window_start = now - self.config.window_seconds

        with self._lock:
            bucket = self._requests_by_key.setdefault(key, deque())
            while bucket and bucket[0] <= window_start:
                bucket.popleft()

            if len(bucket) >= self.config.max_requests:
                retry_after = max(1, int(bucket[0] + self.config.window_seconds - now))
                return RateLimitResult(
                    allowed=False,
                    limit=self.config.max_requests,
                    remaining=0,
                    retry_after_seconds=retry_after,
                    reset_after_seconds=retry_after,
                )

            bucket.append(now)
            remaining = max(self.config.max_requests - len(bucket), 0)
            reset_after = max(1, int(bucket[0] + self.config.window_seconds - now)) if bucket else 0
            return RateLimitResult(
                allowed=True,
                limit=self.config.max_requests,
                remaining=remaining,
                retry_after_seconds=0,
                reset_after_seconds=reset_after,
            )


def config_from_env() -> RateLimitConfig:
    enabled = _env_flag("PNTS_RATE_LIMIT_ENABLED", default=True)
    max_requests = _env_int("PNTS_RATE_LIMIT_MAX_REQUESTS", default=120)
    window_seconds = _env_int("PNTS_RATE_LIMIT_WINDOW_SECONDS", default=60)
    return RateLimitConfig(
        enabled=enabled and max_requests > 0 and window_seconds > 0,
        max_requests=max_requests,
        window_seconds=window_seconds,
    )


def _env_flag(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, *, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return int(raw)
