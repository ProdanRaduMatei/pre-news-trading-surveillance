from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import time
from urllib import error as urllib_error
from urllib import request as urllib_request

from . import snapshot
from ..serve_policy import ServePolicy, parse_datetime

_REMOTE_JSON_CACHE: dict[str, tuple[float, object]] = {}


@dataclass(frozen=True)
class PublishedSnapshotStore:
    root: Path

    def is_available(self) -> bool:
        return (self.root / "manifest.json").exists()

    def manifest(self) -> dict[str, object]:
        return snapshot.load_snapshot_manifest(self.root)

    def evaluation_summary(self) -> dict[str, object] | None:
        return snapshot.load_snapshot_evaluation_summary(self.root)

    def summary(self, *, policy: ServePolicy | None = None) -> dict[str, object]:
        payload = snapshot.load_snapshot_summary(self.root)
        effective_policy = policy or ServePolicy()
        if not effective_policy.public_safe_mode:
            return payload
        return _build_summary_from_events(
            self._filter_events(
                list(snapshot.load_snapshot_events(self.root).get("items", [])),
                policy=effective_policy,
            ),
            base_summary=payload,
        )

    def list_events(
        self,
        *,
        limit: int = 25,
        offset: int = 0,
        ticker: str | None = None,
        event_type: str | None = None,
        min_score: float | None = None,
        policy: ServePolicy | None = None,
    ) -> list[dict[str, object]]:
        payload = snapshot.load_snapshot_events(self.root)
        items = self._filter_events(
            list(payload.get("items", [])),
            ticker=ticker,
            event_type=event_type,
            min_score=min_score,
            policy=policy or ServePolicy(),
        )
        return items[offset : offset + limit]

    def count_events(
        self,
        *,
        ticker: str | None = None,
        event_type: str | None = None,
        min_score: float | None = None,
        policy: ServePolicy | None = None,
    ) -> int:
        payload = snapshot.load_snapshot_events(self.root)
        items = self._filter_events(
            list(payload.get("items", [])),
            ticker=ticker,
            event_type=event_type,
            min_score=min_score,
            policy=policy or ServePolicy(),
        )
        return len(items)

    def get_event(self, event_id: str, *, policy: ServePolicy | None = None) -> dict[str, object] | None:
        item = snapshot.load_snapshot_event(self.root, event_id)
        effective_policy = policy or ServePolicy()
        if item is None:
            return None
        if not effective_policy.is_visible(str(item.get("first_public_at") or "")):
            return None
        return item

    def _load_events(self) -> list[dict[str, object]]:
        return list(snapshot.load_snapshot_events(self.root).get("items", []))

    def _filter_events(
        self,
        items: list[dict[str, object]],
        *,
        ticker: str | None = None,
        event_type: str | None = None,
        min_score: float | None = None,
        policy: ServePolicy,
    ) -> list[dict[str, object]]:
        if policy.public_safe_mode:
            items = [
                item
                for item in items
                if policy.is_visible(str(item.get("first_public_at") or ""))
            ]
        if ticker:
            items = [item for item in items if str(item.get("ticker", "")).upper() == ticker.upper()]
        if event_type:
            items = [item for item in items if item.get("event_type") == event_type]
        if min_score is not None:
            items = [
                item
                for item in items
                if float(item.get("suspiciousness_score") or 0.0) >= min_score
            ]
        return items


@dataclass(frozen=True)
class RemotePublishedSnapshotStore:
    base_url: str
    cache_ttl_seconds: int = 60
    timeout_seconds: float = 5.0

    def is_available(self) -> bool:
        try:
            self.manifest()
        except RuntimeError:
            return False
        return True

    def manifest(self) -> dict[str, object]:
        return self._load_json("manifest.json")

    def evaluation_summary(self) -> dict[str, object] | None:
        return self._load_optional_json("evaluation_summary.json")

    def summary(self, *, policy: ServePolicy | None = None) -> dict[str, object]:
        payload = self._load_json("summary.json")
        effective_policy = policy or ServePolicy()
        if not effective_policy.public_safe_mode:
            return payload
        return _build_summary_from_events(
            self._filter_events(
                self._load_events(),
                policy=effective_policy,
            ),
            base_summary=payload,
        )

    def list_events(
        self,
        *,
        limit: int = 25,
        offset: int = 0,
        ticker: str | None = None,
        event_type: str | None = None,
        min_score: float | None = None,
        policy: ServePolicy | None = None,
    ) -> list[dict[str, object]]:
        items = self._filter_events(
            self._load_events(),
            ticker=ticker,
            event_type=event_type,
            min_score=min_score,
            policy=policy or ServePolicy(),
        )
        return items[offset : offset + limit]

    def count_events(
        self,
        *,
        ticker: str | None = None,
        event_type: str | None = None,
        min_score: float | None = None,
        policy: ServePolicy | None = None,
    ) -> int:
        items = self._filter_events(
            self._load_events(),
            ticker=ticker,
            event_type=event_type,
            min_score=min_score,
            policy=policy or ServePolicy(),
        )
        return len(items)

    def get_event(self, event_id: str, *, policy: ServePolicy | None = None) -> dict[str, object] | None:
        item = self._load_optional_json(f"events/{event_id}.json")
        effective_policy = policy or ServePolicy()
        if item is None:
            return None
        if not effective_policy.is_visible(str(item.get("first_public_at") or "")):
            return None
        return item

    def _load_events(self) -> list[dict[str, object]]:
        payload = self._load_json("events.json")
        return list(payload.get("items", []))

    def _filter_events(
        self,
        items: list[dict[str, object]],
        *,
        ticker: str | None = None,
        event_type: str | None = None,
        min_score: float | None = None,
        policy: ServePolicy,
    ) -> list[dict[str, object]]:
        return _filter_events(
            items,
            ticker=ticker,
            event_type=event_type,
            min_score=min_score,
            policy=policy,
        )

    def _load_json(self, relative_path: str) -> dict[str, object]:
        value = self._fetch_json(relative_path)
        if not isinstance(value, dict):
            raise RuntimeError(f"Snapshot payload at {relative_path} is not a JSON object.")
        return value

    def _load_optional_json(self, relative_path: str) -> dict[str, object] | None:
        try:
            value = self._fetch_json(relative_path)
        except RuntimeError as exc:
            if "404" in str(exc):
                return None
            raise
        if not isinstance(value, dict):
            return None
        return value

    def _fetch_json(self, relative_path: str) -> object:
        url = _join_remote_url(self.base_url, relative_path)
        cached = _REMOTE_JSON_CACHE.get(url)
        now = time.monotonic()
        if cached and (now - cached[0]) <= max(self.cache_ttl_seconds, 0):
            return cached[1]

        request = urllib_request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "pre-news-trading-surveillance/0.1",
            },
        )
        try:
            with urllib_request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib_error.HTTPError as exc:
            raise RuntimeError(f"Failed to fetch published snapshot asset {url}: HTTP {exc.code}") from exc
        except urllib_error.URLError as exc:
            raise RuntimeError(f"Failed to fetch published snapshot asset {url}: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Published snapshot asset {url} did not contain valid JSON.") from exc

        _REMOTE_JSON_CACHE[url] = (now, payload)
        return payload


def _build_summary_from_events(
    items: list[dict[str, object]],
    *,
    base_summary: dict[str, object],
) -> dict[str, object]:
    carried_fields = {
        key: value
        for key, value in base_summary.items()
        if key not in {"overview", "score_bands", "event_types", "top_tickers", "recent_activity"}
    }
    overview_base = dict(base_summary.get("overview", {}))
    dated_items = _sorted_datetimes(items)
    if not items or not dated_items:
        overview_base.update(
            {
                "total_events": 0,
                "tracked_tickers": 0,
                "coverage_start": None,
                "coverage_end": None,
                "average_score": 0.0,
                "peak_score": 0.0,
                "high_risk_events": 0,
                "medium_risk_events": 0,
                "low_risk_events": 0,
            }
        )
        return {
            **carried_fields,
            "overview": overview_base,
            "score_bands": [],
            "event_types": [],
            "top_tickers": [],
            "recent_activity": [],
        }

    scores = [float(item.get("suspiciousness_score") or 0.0) for item in items]
    overview_base.update(
        {
            "total_events": len(items),
            "tracked_tickers": len({str(item.get("ticker") or "") for item in items if item.get("ticker")}),
            "coverage_start": dated_items[0][0],
            "coverage_end": dated_items[-1][0],
            "average_score": round(sum(scores) / len(scores), 2),
            "peak_score": round(max(scores), 2),
            "high_risk_events": sum(1 for item in items if item.get("score_band") == "High"),
            "medium_risk_events": sum(1 for item in items if item.get("score_band") == "Medium"),
            "low_risk_events": sum(1 for item in items if item.get("score_band") == "Low"),
        }
    )
    return {
        **carried_fields,
        "overview": overview_base,
        "score_bands": _aggregate_counts(items, key="score_band", default="Unscored", top_n=None),
        "event_types": _aggregate_counts(items, key="event_type", default="unknown", top_n=8),
        "top_tickers": _aggregate_counts(items, key="ticker", default="UNKNOWN", top_n=8),
        "recent_activity": _aggregate_recent_activity(items),
    }


def _aggregate_counts(
    items: list[dict[str, object]],
    *,
    key: str,
    default: str,
    top_n: int | None,
) -> list[dict[str, object]]:
    stats: dict[str, dict[str, float]] = {}
    for item in items:
        bucket = str(item.get(key) or default)
        score = float(item.get("suspiciousness_score") or 0.0)
        current = stats.setdefault(bucket, {"count": 0, "sum": 0.0, "max": 0.0})
        current["count"] += 1
        current["sum"] += score
        current["max"] = max(current["max"], score)

    label_name = "score_band" if key == "score_band" else key
    rows = []
    for bucket, values in stats.items():
        row = {
            label_name: bucket,
            "event_count": int(values["count"]),
            "average_score": round(values["sum"] / values["count"], 2),
        }
        if key != "score_band":
            row["peak_score"] = round(values["max"], 2)
        rows.append(row)

    if key == "score_band":
        order = {"High": 0, "Medium": 1, "Low": 2, "Unscored": 3}
        rows.sort(key=lambda row: order.get(str(row["score_band"]), 99))
    else:
        rows.sort(
            key=lambda row: (
                -int(row["event_count"]),
                -float(row.get("peak_score", row.get("average_score", 0.0))),
                str(row[label_name]),
            )
        )
    return rows if top_n is None else rows[:top_n]


def _aggregate_recent_activity(items: list[dict[str, object]]) -> list[dict[str, object]]:
    stats: dict[str, dict[str, float]] = {}
    for item in items:
        published_at = parse_datetime(str(item.get("first_public_at") or ""))
        if published_at is None:
            continue
        day = published_at.date().isoformat()
        score = float(item.get("suspiciousness_score") or 0.0)
        current = stats.setdefault(day, {"count": 0, "sum": 0.0})
        current["count"] += 1
        current["sum"] += score

    rows = [
        {
            "event_day": day,
            "event_count": int(values["count"]),
            "average_score": round(values["sum"] / values["count"], 2),
        }
        for day, values in stats.items()
    ]
    rows.sort(key=lambda row: str(row["event_day"]))
    return rows[-10:]


def _sorted_datetimes(items: list[dict[str, object]]) -> list[tuple[str, object]]:
    values = []
    for item in items:
        raw = str(item.get("first_public_at") or "")
        parsed = parse_datetime(raw)
        if parsed is not None:
            values.append((raw, parsed))
    values.sort(key=lambda pair: pair[1])
    return values


def _filter_events(
    items: list[dict[str, object]],
    *,
    ticker: str | None = None,
    event_type: str | None = None,
    min_score: float | None = None,
    policy: ServePolicy,
) -> list[dict[str, object]]:
    if policy.public_safe_mode:
        items = [
            item
            for item in items
            if policy.is_visible(str(item.get("first_public_at") or ""))
        ]
    if ticker:
        items = [item for item in items if str(item.get("ticker", "")).upper() == ticker.upper()]
    if event_type:
        items = [item for item in items if item.get("event_type") == event_type]
    if min_score is not None:
        items = [
            item
            for item in items
            if float(item.get("suspiciousness_score") or 0.0) >= min_score
        ]
    return items


def _join_remote_url(base_url: str, relative_path: str) -> str:
    return f"{base_url.rstrip('/')}/{relative_path.lstrip('/')}"
