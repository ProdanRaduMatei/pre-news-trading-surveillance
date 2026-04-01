from __future__ import annotations

from contextlib import asynccontextmanager
from importlib.resources import files
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .. import db
from ..publish.store import PublishedSnapshotStore
from ..serve_policy import ServePolicy, policy_from_env
from ..settings import default_paths
from .rate_limit import InMemoryRateLimiter, config_from_env

UI_PACKAGE = "pre_news_trading_surveillance.ui"


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    paths = default_paths()
    if _data_source_mode() != "published":
        paths.ensure_directories()
        db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)
    app_instance.state.rate_limiter = InMemoryRateLimiter(config_from_env())
    yield


app = FastAPI(
    title="Pre-News Trading Surveillance API",
    version="0.1.0",
    description="Public-facing research API for unusual pre-disclosure market activity.",
    lifespan=lifespan,
)
app.mount("/static", StaticFiles(packages=[(UI_PACKAGE, "static")]), name="static")


@app.middleware("http")
async def apply_public_api_controls(request: Request, call_next):
    limiter = getattr(request.app.state, "rate_limiter", InMemoryRateLimiter(config_from_env()))
    path = request.url.path
    if not _is_rate_limit_exempt(path):
        result = limiter.check(_client_identifier(request))
        if not result.allowed:
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Rate limit exceeded. Please retry after the public cache window rolls over.",
                },
                headers={
                    "Retry-After": str(result.retry_after_seconds),
                    "X-RateLimit-Limit": str(result.limit),
                    "X-RateLimit-Remaining": str(result.remaining),
                    "X-RateLimit-Reset": str(result.reset_after_seconds),
                },
            )
    else:
        result = None

    response = await call_next(request)
    cache_control = _cache_control_for_path(path)
    if cache_control:
        response.headers["Cache-Control"] = cache_control
    if result is not None:
        response.headers["X-RateLimit-Limit"] = str(result.limit)
        response.headers["X-RateLimit-Remaining"] = str(result.remaining)
        response.headers["X-RateLimit-Reset"] = str(result.reset_after_seconds)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    return response


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
    policy = _serve_policy()
    if store is not None:
        payload = store.summary(policy=policy)
        manifest = store.manifest()
    else:
        payload = db.get_dashboard_summary(
            paths.db_path,
            max_first_public_at=policy.cutoff_at_iso(),
        )
        manifest = None

    payload["api"] = {"name": app.title, "version": app.version}
    payload["policy"] = policy.metadata()
    if manifest is not None:
        payload["manifest"] = manifest
    return payload


@app.get("/events")
def list_events(
    limit: int = Query(default=25, ge=1, le=250),
    offset: int = Query(default=0, ge=0, le=5000),
    ticker: str | None = None,
    event_type: str | None = None,
    min_score: float | None = Query(default=None, ge=0, le=100),
) -> dict[str, object]:
    paths = default_paths()
    store = _published_store(paths)
    policy = _serve_policy()
    visible_before = policy.cutoff_at_iso()

    if store is not None:
        total = store.count_events(
            ticker=ticker,
            event_type=event_type,
            min_score=min_score,
            policy=policy,
        )
        events = store.list_events(
            limit=limit,
            offset=offset,
            ticker=ticker,
            event_type=event_type,
            min_score=min_score,
            policy=policy,
        )
    else:
        total = db.count_ranked_events(
            paths.db_path,
            ticker=ticker,
            event_type=event_type,
            min_score=min_score,
            max_first_public_at=visible_before,
        )
        events = db.list_ranked_events(
            db_path=paths.db_path,
            limit=limit,
            offset=offset,
            ticker=ticker,
            event_type=event_type,
            min_score=min_score,
            max_first_public_at=visible_before,
        )

    next_offset = offset + limit if (offset + limit) < total else None
    previous_offset = max(offset - limit, 0) if offset > 0 else None
    return {
        "items": events,
        "count": len(events),
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": next_offset is not None,
        "next_offset": next_offset,
        "previous_offset": previous_offset,
        "policy": policy.metadata(),
    }


@app.get("/events/{event_id}")
def get_event(event_id: str) -> dict[str, object]:
    paths = default_paths()
    store = _published_store(paths)
    policy = _serve_policy()
    if store is not None:
        event = store.get_event(event_id, policy=policy)
    else:
        event = db.get_ranked_event(
            paths.db_path,
            event_id,
            max_first_public_at=policy.cutoff_at_iso(),
        )
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    event["policy"] = policy.metadata()
    return event


@app.get("/ingestion-runs")
def list_ingestion_runs(
    limit: int = Query(default=25, ge=1, le=250),
    pipeline_name: str | None = None,
    status: str | None = None,
) -> dict[str, object]:
    paths = default_paths()
    store = _published_store(paths)
    policy = _serve_policy()
    if store is not None or policy.public_safe_mode:
        raise HTTPException(
            status_code=503,
            detail="Operational run visibility is disabled on the public research surface.",
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


def _serve_policy() -> ServePolicy:
    return policy_from_env(data_source_mode=_data_source_mode())


def _is_rate_limit_exempt(path: str) -> bool:
    return path in {"/health", "/openapi.json"} or path.startswith("/docs") or path.startswith("/static")


def _client_identifier(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _cache_control_for_path(path: str) -> str:
    if path == "/health":
        return "no-store"
    if path.startswith("/ingestion-runs"):
        return "private, no-store"
    if path.startswith("/static"):
        return "public, max-age=86400, stale-while-revalidate=604800"
    if path == "/":
        return "public, max-age=0, s-maxage=60, stale-while-revalidate=300"
    if path.startswith("/summary") or path.startswith("/events"):
        return "public, max-age=0, s-maxage=60, stale-while-revalidate=300"
    return "no-store"
