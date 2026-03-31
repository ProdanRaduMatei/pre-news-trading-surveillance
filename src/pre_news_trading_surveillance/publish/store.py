from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from . import snapshot


@dataclass(frozen=True)
class PublishedSnapshotStore:
    root: Path

    def is_available(self) -> bool:
        return (self.root / "manifest.json").exists()

    def summary(self) -> dict[str, object]:
        return snapshot.load_snapshot_summary(self.root)

    def list_events(
        self,
        *,
        limit: int = 25,
        ticker: str | None = None,
        event_type: str | None = None,
        min_score: float | None = None,
    ) -> list[dict[str, object]]:
        payload = snapshot.load_snapshot_events(self.root)
        items = list(payload.get("items", []))
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
        return items[:limit]

    def get_event(self, event_id: str) -> dict[str, object] | None:
        return snapshot.load_snapshot_event(self.root, event_id)
