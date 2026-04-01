from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    docs_dir: Path
    configs_dir: Path
    sql_dir: Path
    src_dir: Path
    tests_dir: Path
    data_dir: Path
    raw_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    publish_dir: Path
    db_path: Path

    def ensure_directories(self) -> None:
        for path in (
            self.docs_dir,
            self.configs_dir,
            self.sql_dir,
            self.src_dir,
            self.tests_dir,
            self.data_dir,
            self.raw_dir,
            self.bronze_dir,
            self.silver_dir,
            self.gold_dir,
            self.publish_dir,
            self.publish_dir / "current",
            self.raw_dir / "sec",
            self.raw_dir / "sec" / "submissions",
            self.raw_dir / "market",
            self.raw_dir / "market" / "csv",
            self.raw_dir / "market" / "csv" / "daily",
            self.raw_dir / "market" / "csv" / "minute",
            self.raw_dir / "market" / "alpha_vantage",
            self.raw_dir / "market" / "alpha_vantage" / "daily",
            self.raw_dir / "market" / "alpha_vantage" / "minute",
            self.bronze_dir / "sec",
            self.silver_dir / "events",
            self.silver_dir / "features",
            self.silver_dir / "features" / "daily",
            self.silver_dir / "features" / "minute",
            self.silver_dir / "scoring",
        ):
            path.mkdir(parents=True, exist_ok=True)


def default_paths(root: Path | None = None) -> ProjectPaths:
    project_root = root or Path(__file__).resolve().parents[2]
    data_dir = project_root / "data"
    return ProjectPaths(
        root=project_root,
        docs_dir=project_root / "docs",
        configs_dir=project_root / "configs",
        sql_dir=project_root / "sql",
        src_dir=project_root / "src",
        tests_dir=project_root / "tests",
        data_dir=data_dir,
        raw_dir=data_dir / "raw",
        bronze_dir=data_dir / "bronze",
        silver_dir=data_dir / "silver",
        gold_dir=data_dir / "gold",
        publish_dir=data_dir / "publish",
        db_path=data_dir / "gold" / "pnts.duckdb",
    )
