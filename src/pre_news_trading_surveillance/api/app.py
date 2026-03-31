from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from .. import db
from ..settings import default_paths

app = FastAPI(
    title="Pre-News Trading Surveillance API",
    version="0.1.0",
    description="Public-facing research API for unusual pre-disclosure market activity.",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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
