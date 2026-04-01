from __future__ import annotations

from contextlib import asynccontextmanager
from importlib.resources import files
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .. import db
from ..publish.store import PublishedSnapshotStore
from ..settings import default_paths

UI_PACKAGE = "pre_news_trading_surveillance.ui"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    paths = default_paths()
    paths.ensure_directories()
    if _data_source_mode() != "published":
        db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
    yield


app = FastAPI(
    title="Pre-News Trading Surveillance API",
    version="0.1.0",
    description="Public-facing research API for unusual pre-disclosure market activity.",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(packages=[(UI_PACKAGE, "static")]), name="static")


@app.get("/", include_in_schema=False)
def dashboard() -> HTMLResponse:
    index_html = files(UI_PACKAGE).joinpath("static/index.html").read_text(encoding="utf-8")
    return HTMLResponse(index_html)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/summary")
def summary() -> dict[str, object]:
    paths = default_paths()
    store = _published_store(paths)
    if store is not None:
        payload = store.summary()
    else:
        payload = db.get_dashboard_summary(paths.db_path)
    payload["api"] = {"name": app.title, "version": app.version}
    return payload


@app.get("/events")
def list_events(
    limit: int = Query(default=25, ge=1, le=250),
    ticker: str | None = None,
    event_type: str | None = None,
    min_score: float | None = Query(default=None, ge=0, le=100),
) -> dict[str, object]:
    paths = default_paths()
    store = _published_store(paths)
    if store is not None:
        events = store.list_events(
            limit=limit,
            ticker=ticker,
            event_type=event_type,
            min_score=min_score,
        )
    else:
        events = db.list_ranked_events(
            db_path=paths.db_path,
            limit=limit,
            ticker=ticker,
            event_type=event_type,
            min_score=min_score,
        )
    return {"items": events, "count": len(events)}


@app.get("/events/{event_id}")
def get_event(event_id: str) -> dict[str, object]:
    paths = default_paths()
    store = _published_store(paths)
    if store is not None:
        event = store.get_event(event_id)
    else:
        event = db.get_ranked_event(paths.db_path, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@app.get("/ingestion-runs")
def list_ingestion_runs(
    limit: int = Query(default=25, ge=1, le=250),
    pipeline_name: str | None = None,
    status: str | None = None,
) -> dict[str, object]:
    paths = default_paths()
    store = _published_store(paths)
    if store is not None:
        raise HTTPException(
            status_code=503,
            detail="Ingestion run visibility is only available in DuckDB mode.",
        )
    items = db.list_ingestion_runs(
        paths.db_path,
        limit=limit,
        pipeline_name=pipeline_name,
        status=status,
    )
    return {"items": items, "count": len(items)}


def _data_source_mode() -> str:
    return os.getenv("PNTS_API_DATA_SOURCE", "duckdb").strip().lower() or "duckdb"


def _published_store(paths) -> PublishedSnapshotStore | None:
    if _data_source_mode() != "published":
        return None
    root = Path(os.getenv("PNTS_PUBLISHED_DATA_DIR", str(paths.publish_dir / "current")))
    store = PublishedSnapshotStore(root)
    if not store.is_available():
        raise RuntimeError(
            f"Published snapshot mode is enabled but no manifest was found at {root}."
        )
    return store
