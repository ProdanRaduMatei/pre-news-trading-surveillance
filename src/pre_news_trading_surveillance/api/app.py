from __future__ import annotations

from contextlib import asynccontextmanager
from importlib.resources import files
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .. import db
from ..evaluation.public_summary import load_public_evaluation_summary
from ..publish.store import PublishedSnapshotStore, RemotePublishedSnapshotStore
from ..serve_policy import ServePolicy, parse_datetime, policy_from_env
from ..settings import default_paths
from ..ui.docs import render_markdown_page
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


@app.get("/methodology", include_in_schema=False)
def methodology_page() -> HTMLResponse:
    paths = default_paths()
    return HTMLResponse(
        render_markdown_page(
            title="Methodology",
            eyebrow="Research Methodology",
            intro=(
                "How the platform builds official events, computes market anomalies, and serves a "
                "public-safe ranking surface."
            ),
            markdown_text=_load_doc_markdown(paths.docs_dir / "METHODOLOGY.md"),
            actions=[
                ("/", "Back to Dashboard"),
                ("/evaluation", "View Evaluation"),
                ("/limitations", "View Limitations"),
            ],
            side_panel_title="Product boundary",
            side_panel_body=(
                "This product ranks unusual public-data patterns around official events. It does not "
                "identify traders or establish legal wrongdoing."
            ),
        )
    )


@app.get("/limitations", include_in_schema=False)
def limitations_page() -> HTMLResponse:
    paths = default_paths()
    return HTMLResponse(
        render_markdown_page(
            title="Risk and Limitations",
            eyebrow="Research Limitations",
            intro=(
                "The major product, data, and interpretation boundaries users should understand before "
                "acting on any ranked event."
            ),
            markdown_text=_load_doc_markdown(paths.docs_dir / "RISK_AND_LIMITATIONS.md"),
            actions=[
                ("/", "Back to Dashboard"),
                ("/methodology", "View Methodology"),
                ("/evaluation", "View Evaluation"),
            ],
            side_panel_title="Interpretation caution",
            side_panel_body=(
                "A high score indicates a stronger research signal for review. It is not proof of insider "
                "trading, misconduct, or a complete explanation of what happened."
            ),
        )
    )


@app.get("/evaluation", include_in_schema=False)
def evaluation_page() -> HTMLResponse:
    paths = default_paths()
    store = _published_store(paths)
    evaluation = _evaluation_summary(paths, store)
    return HTMLResponse(
        render_markdown_page(
            title="Evaluation",
            eyebrow="Model Evaluation",
            intro=(
                "How the ranking stack is benchmarked, what the reported metrics mean, and how much "
                "reviewed evidence is currently behind the published system."
            ),
            markdown_text=_load_doc_markdown(paths.docs_dir / "EVALUATION.md"),
            actions=[
                ("/", "Back to Dashboard"),
                ("/methodology", "View Methodology"),
                ("/limitations", "View Limitations"),
            ],
            side_panel_title="Evaluation posture",
            side_panel_body=(
                "The goal is ranking quality on reviewed historical events. These metrics are not claims "
                "about criminal truth and should be read as surveillance performance measures."
            ),
            extra_html=_build_evaluation_page_summary(evaluation),
        )
    )


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
    payload["evaluation"] = _evaluation_summary(paths, store)
    if manifest is not None:
        payload["manifest"] = manifest
    return payload


@app.get("/evaluation/summary")
def evaluation_summary() -> dict[str, object]:
    paths = default_paths()
    store = _published_store(paths)
    return {
        "evaluation": _evaluation_summary(paths, store),
        "policy": _serve_policy().metadata(),
    }


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
    base_url = os.getenv("PNTS_PUBLISHED_DATA_BASE_URL", "").strip()
    if base_url:
        store = RemotePublishedSnapshotStore(
            base_url=base_url,
            cache_ttl_seconds=max(int(os.getenv("PNTS_REMOTE_PUBLISHED_CACHE_SECONDS", "60")), 0),
            timeout_seconds=max(float(os.getenv("PNTS_REMOTE_PUBLISHED_TIMEOUT_SECONDS", "5")), 0.5),
        )
        if not store.is_available():
            raise RuntimeError(
                f"Published snapshot mode is enabled but no manifest could be loaded from {base_url}."
            )
        return store
    root = Path(os.getenv("PNTS_PUBLISHED_DATA_DIR", str(paths.publish_dir / "current")))
    store = PublishedSnapshotStore(root)
    if not store.is_available():
        raise RuntimeError(
            f"Published snapshot mode is enabled but no manifest was found at {root}."
        )
    return store


def _serve_policy() -> ServePolicy:
    return policy_from_env(data_source_mode=_data_source_mode())


def _evaluation_summary(
    paths,
    store: PublishedSnapshotStore | RemotePublishedSnapshotStore | None,
) -> dict[str, object]:
    if store is not None:
        return store.evaluation_summary() or {
            "status": "pending",
            "notice": "No published evaluation summary is bundled with the current snapshot.",
            "benchmark": {},
            "overall": {},
            "hybrid": {},
            "ablations": [],
            "source": {},
            "generated_at": None,
        }
    return load_public_evaluation_summary(paths.db_path)


def _load_doc_markdown(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8")
    fallback = Path(__file__).resolve().parents[3] / "docs" / path.name
    return fallback.read_text(encoding="utf-8")


def _build_evaluation_page_summary(evaluation: dict[str, object]) -> str:
    benchmark = dict(evaluation.get("benchmark") or {})
    hybrid = dict(evaluation.get("hybrid") or {})
    precision_at = dict(hybrid.get("precision_at") or {})
    generated_at = evaluation.get("generated_at")
    if evaluation.get("status") != "available":
        return f"""
        <section class="panel reveal">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Evaluation Status</p>
              <h2>Reviewed benchmark not yet published</h2>
            </div>
          </div>
          <p class="panel-caption">{evaluation.get("notice") or "Evaluation results are pending."}</p>
        </section>
        """
    return f"""
    <section class="insight-grid reveal">
      <article class="panel">
        <div class="panel-header">
          <div>
            <p class="panel-kicker">Benchmark Coverage</p>
            <h2>{_format_int(benchmark.get("reviewed_events"))} reviewed events</h2>
          </div>
          <p class="panel-caption">Generated { _format_datetime(str(generated_at or '')) }</p>
        </div>
        <div class="evaluation-grid">
          <article class="detail-stat">
            <span class="detail-label">Suspicious labels</span>
            <strong>{_format_int(benchmark.get("positive_labels"))}</strong>
          </article>
          <article class="detail-stat">
            <span class="detail-label">Control labels</span>
            <strong>{_format_int(benchmark.get("control_labels"))}</strong>
          </article>
          <article class="detail-stat">
            <span class="detail-label">Chronological folds</span>
            <strong>{_format_int(benchmark.get("fold_count"))}</strong>
          </article>
          <article class="detail-stat">
            <span class="detail-label">Hybrid top-decile lift</span>
            <strong>{_format_metric(hybrid.get("top_decile_lift"))}</strong>
          </article>
        </div>
      </article>
      <article class="panel">
        <div class="panel-header">
          <div>
            <p class="panel-kicker">Hybrid Metrics</p>
            <h2>Ranking quality snapshot</h2>
          </div>
        </div>
        <div class="evaluation-grid">
          <article class="detail-stat">
            <span class="detail-label">Precision@5</span>
            <strong>{_format_metric(precision_at.get("5"))}</strong>
          </article>
          <article class="detail-stat">
            <span class="detail-label">Precision@10</span>
            <strong>{_format_metric(precision_at.get("10"))}</strong>
          </article>
          <article class="detail-stat">
            <span class="detail-label">Precision@25</span>
            <strong>{_format_metric(precision_at.get("25"))}</strong>
          </article>
          <article class="detail-stat">
            <span class="detail-label">Evaluated events</span>
            <strong>{_format_int(hybrid.get("evaluated_events"))}</strong>
          </article>
        </div>
      </article>
    </section>
    """


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
    if path in {"/methodology", "/limitations", "/evaluation"}:
        return "public, max-age=0, s-maxage=300, stale-while-revalidate=86400"
    if path == "/":
        return "public, max-age=0, s-maxage=60, stale-while-revalidate=300"
    if path.startswith("/summary") or path.startswith("/events") or path.startswith("/evaluation/summary"):
        return "public, max-age=0, s-maxage=60, stale-while-revalidate=300"
    return "no-store"


def _format_metric(value: object) -> str:
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return "n/a"


def _format_int(value: object) -> str:
    try:
        return f"{int(value)}"
    except (TypeError, ValueError):
        return "n/a"


def _format_datetime(value: str) -> str:
    parsed = parse_datetime(value)
    if parsed is None:
        return "Unavailable"
    return parsed.strftime("%b %d, %Y %H:%M UTC")
