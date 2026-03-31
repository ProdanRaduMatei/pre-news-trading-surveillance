from __future__ import annotations

from contextlib import asynccontextmanager
from importlib.resources import files

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .. import db
from ..settings import default_paths

UI_PACKAGE = "pre_news_trading_surveillance.ui"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    paths = default_paths()
    paths.ensure_directories()
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
    event = db.get_ranked_event(paths.db_path, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return event
