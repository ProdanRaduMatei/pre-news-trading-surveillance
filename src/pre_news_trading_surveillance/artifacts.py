from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def copy_snapshot(source_path: Path, output_dir: Path, *, name_prefix: str | None = None) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = _timestamp_slug()
    prefix = _safe_prefix(name_prefix or source_path.stem)
    suffix = source_path.suffix or ".bin"
    target = output_dir / f"{prefix}_{timestamp}{suffix}"
    shutil.copy2(source_path, target)
    return target


def write_ndjson_snapshot(
    output_dir: Path,
    *,
    name_prefix: str,
    rows: list[dict[str, object]],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{_safe_prefix(name_prefix)}_{_timestamp_slug()}.ndjson"
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    return path


def write_json_snapshot(
    output_dir: Path,
    *,
    name_prefix: str,
    payload: dict[str, object],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{_safe_prefix(name_prefix)}_{_timestamp_slug()}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_prefix(value: str) -> str:
    return "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in value)
