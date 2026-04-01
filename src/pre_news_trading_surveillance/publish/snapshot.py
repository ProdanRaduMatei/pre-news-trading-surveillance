from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .. import db
from ..evaluation.public_summary import load_public_evaluation_summary
from ..serve_policy import ServePolicy


@dataclass(frozen=True)
class SnapshotBundle:
    manifest: dict[str, object]
    summary: dict[str, object]
    evaluation_summary: dict[str, object] | None
    events: list[dict[str, object]]
    details: dict[str, dict[str, object]]


def build_snapshot_bundle(
    *,
    db_path: Path,
    events_limit: int = 250,
    policy: ServePolicy | None = None,
) -> SnapshotBundle:
    effective_policy = policy or ServePolicy()
    visible_before = effective_policy.cutoff_at_iso()
    summary = db.get_dashboard_summary(db_path, max_first_public_at=visible_before)
    events = db.list_ranked_events(
        db_path=db_path,
        limit=events_limit,
        offset=0,
        min_score=0,
        max_first_public_at=visible_before,
    )
    details = {
        str(event["event_id"]): db.get_ranked_event(
            db_path,
            str(event["event_id"]),
            max_first_public_at=visible_before,
        )
        or {}
        for event in events
    }
    generated_at = _utc_now_iso()
    evaluation_summary = load_public_evaluation_summary(db_path)
    manifest = {
        "generated_at": generated_at,
        "events_limit": events_limit,
        "events_count": len(events),
        "format_version": 1,
        "policy": effective_policy.metadata(),
        "evaluation_status": (evaluation_summary or {}).get("status"),
    }
    return SnapshotBundle(
        manifest=manifest,
        summary=summary,
        evaluation_summary=evaluation_summary,
        events=events,
        details=details,
    )


def write_snapshot_bundle(bundle: SnapshotBundle, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    events_dir = output_dir / "events"
    events_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / "manifest.json").write_text(
        json.dumps(bundle.manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "summary.json").write_text(
        json.dumps(bundle.summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "evaluation_summary.json").write_text(
        json.dumps(bundle.evaluation_summary or {}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "events.json").write_text(
        json.dumps(
            {
                "items": bundle.events,
                "count": len(bundle.events),
                "policy": bundle.manifest.get("policy", {}),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    for event_id, payload in bundle.details.items():
        (events_dir / f"{event_id}.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    return output_dir


def load_snapshot_manifest(output_dir: Path) -> dict[str, object]:
    return json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))


def load_snapshot_summary(output_dir: Path) -> dict[str, object]:
    return json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))


def load_snapshot_events(output_dir: Path) -> dict[str, object]:
    return json.loads((output_dir / "events.json").read_text(encoding="utf-8"))


def load_snapshot_evaluation_summary(output_dir: Path) -> dict[str, object] | None:
    path = output_dir / "evaluation_summary.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_snapshot_event(output_dir: Path, event_id: str) -> dict[str, object] | None:
    path = output_dir / "events" / f"{event_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
