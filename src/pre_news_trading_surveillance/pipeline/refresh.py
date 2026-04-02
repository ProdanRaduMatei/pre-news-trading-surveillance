from __future__ import annotations

import os
import tomllib
from argparse import Namespace
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .. import db

RefreshStep = str
FULL_REFRESH_STEPS: list[RefreshStep] = [
    "sec_reference",
    "sec_filings",
    "press_releases",
    "market_daily",
    "market_minute",
    "build_events",
    "compute_daily",
    "compute_minute",
    "train_model",
    "score",
    "backtest",
    "publish",
]
INTRADAY_REFRESH_STEPS: list[RefreshStep] = [
    "sec_filings",
    "press_releases",
    "market_minute",
    "build_events",
    "compute_minute",
    "score",
    "publish",
]
VALID_REFRESH_STEPS = set(FULL_REFRESH_STEPS)


@dataclass(frozen=True)
class SecRefreshConfig:
    user_agent: str | None
    user_agent_env: str
    refresh_reference: bool
    per_ticker_limit: int
    forms: list[str]


@dataclass(frozen=True)
class MarketProviderConfig:
    provider: str
    api_key: str | None
    api_key_env: str
    timeout_seconds: int


@dataclass(frozen=True)
class IssuerReleaseRefreshConfig:
    enabled: bool
    config_path: str | None
    config_path_env: str
    user_agent: str | None
    user_agent_env: str
    per_feed_limit: int


@dataclass(frozen=True)
class MarketDailyRefreshConfig:
    enabled: bool
    outputsize: str


@dataclass(frozen=True)
class MarketMinuteRefreshConfig:
    enabled: bool
    interval: str
    outputsize: str
    month: str | None
    entitlement: str | None
    adjusted: bool
    extended_hours: bool


@dataclass(frozen=True)
class NlpRefreshConfig:
    sentiment_backend: str
    sentiment_model: str | None
    novelty_backend: str
    novelty_model: str | None


@dataclass(frozen=True)
class ModelRefreshConfig:
    enabled: bool
    output_dir: str
    contamination: float
    min_samples: int
    use_ranker: bool
    review_status: str
    benchmark_labels: list[str]
    reviewer: str | None


@dataclass(frozen=True)
class EvaluationRefreshConfig:
    enabled: bool
    review_status: str
    benchmark_labels: list[str]
    reviewer: str | None
    folds: int
    min_train_size: int
    k_values: list[int]
    contamination: float
    use_ranker: bool
    output_dir: str


@dataclass(frozen=True)
class PublishRefreshConfig:
    enabled: bool
    output_dir: str
    max_events: int
    public_safe_mode: bool
    public_delay_minutes: int
    s3_enabled: bool
    s3_bucket: str | None
    s3_bucket_env: str
    s3_prefix: str
    s3_region: str | None
    s3_region_env: str
    s3_endpoint_url: str | None
    s3_endpoint_url_env: str
    s3_access_key_env: str
    s3_secret_key_env: str
    s3_session_token_env: str


@dataclass(frozen=True)
class RefreshPipelineConfig:
    tickers: list[str]
    sec: SecRefreshConfig
    issuer_releases: IssuerReleaseRefreshConfig
    market: MarketProviderConfig
    market_daily: MarketDailyRefreshConfig
    market_minute: MarketMinuteRefreshConfig
    nlp: NlpRefreshConfig
    model: ModelRefreshConfig
    evaluation: EvaluationRefreshConfig
    publish: PublishRefreshConfig


def load_refresh_config(config_path: Path) -> RefreshPipelineConfig:
    payload = tomllib.loads(config_path.read_text(encoding="utf-8"))

    tickers = [ticker.upper() for ticker in payload.get("tickers", [])]
    if not tickers:
        raise ValueError("Refresh config must define at least one ticker.")

    sec_payload = payload.get("sec", {})
    issuer_release_payload = payload.get("issuer_releases", {})
    market_payload = payload.get("market", {})
    market_daily_payload = market_payload.get("daily", {})
    market_minute_payload = market_payload.get("minute", {})
    nlp_payload = payload.get("nlp", {})
    model_payload = payload.get("model", {})
    evaluation_payload = payload.get("evaluation", {})
    publish_payload = payload.get("publish", {})

    return RefreshPipelineConfig(
        tickers=tickers,
        sec=SecRefreshConfig(
            user_agent=_none_if_blank(sec_payload.get("user_agent")),
            user_agent_env=str(sec_payload.get("user_agent_env", "SEC_USER_AGENT")),
            refresh_reference=bool(sec_payload.get("refresh_reference", True)),
            per_ticker_limit=int(sec_payload.get("per_ticker_limit", 50)),
            forms=[str(form).upper() for form in sec_payload.get("forms", ["8-K", "6-K"])],
        ),
        issuer_releases=IssuerReleaseRefreshConfig(
            enabled=bool(issuer_release_payload.get("enabled", False)),
            config_path=_none_if_blank(issuer_release_payload.get("config_path")),
            config_path_env=str(
                issuer_release_payload.get("config_path_env", "PNTS_ISSUER_FEED_CONFIG")
            ),
            user_agent=_none_if_blank(issuer_release_payload.get("user_agent")),
            user_agent_env=str(
                issuer_release_payload.get("user_agent_env", "PRESS_RELEASES_USER_AGENT")
            ),
            per_feed_limit=int(issuer_release_payload.get("per_feed_limit", 25)),
        ),
        market=MarketProviderConfig(
            provider=str(market_payload.get("provider", "alpha_vantage")),
            api_key=_none_if_blank(market_payload.get("api_key")),
            api_key_env=str(market_payload.get("api_key_env", "ALPHAVANTAGE_API_KEY")),
            timeout_seconds=int(market_payload.get("timeout_seconds", 30)),
        ),
        market_daily=MarketDailyRefreshConfig(
            enabled=bool(market_daily_payload.get("enabled", True)),
            outputsize=str(market_daily_payload.get("outputsize", "compact")),
        ),
        market_minute=MarketMinuteRefreshConfig(
            enabled=bool(market_minute_payload.get("enabled", True)),
            interval=str(market_minute_payload.get("interval", "1min")),
            outputsize=str(market_minute_payload.get("outputsize", "compact")),
            month=_none_if_blank(market_minute_payload.get("month")),
            entitlement=_none_if_blank(market_minute_payload.get("entitlement")),
            adjusted=bool(market_minute_payload.get("adjusted", False)),
            extended_hours=bool(market_minute_payload.get("extended_hours", True)),
        ),
        nlp=NlpRefreshConfig(
            sentiment_backend=str(nlp_payload.get("sentiment_backend", "heuristic")),
            sentiment_model=_none_if_blank(nlp_payload.get("sentiment_model")),
            novelty_backend=str(nlp_payload.get("novelty_backend", "lexical")),
            novelty_model=_none_if_blank(nlp_payload.get("novelty_model")),
        ),
        model=ModelRefreshConfig(
            enabled=bool(model_payload.get("enabled", True)),
            output_dir=str(model_payload.get("output_dir", "data/models/scoring/current")),
            contamination=float(model_payload.get("contamination", 0.12)),
            min_samples=int(model_payload.get("min_samples", 12)),
            use_ranker=bool(model_payload.get("use_ranker", True)),
            review_status=str(model_payload.get("review_status", "reviewed")),
            benchmark_labels=[
                str(label).strip().lower()
                for label in model_payload.get("benchmark_labels", ["suspicious", "control"])
            ],
            reviewer=_none_if_blank(model_payload.get("reviewer")),
        ),
        evaluation=EvaluationRefreshConfig(
            enabled=bool(evaluation_payload.get("enabled", True)),
            review_status=str(evaluation_payload.get("review_status", "reviewed")),
            benchmark_labels=[
                str(label).strip().lower()
                for label in evaluation_payload.get("benchmark_labels", ["suspicious", "control"])
            ],
            reviewer=_none_if_blank(evaluation_payload.get("reviewer")),
            folds=int(evaluation_payload.get("folds", 3)),
            min_train_size=int(evaluation_payload.get("min_train_size", 12)),
            k_values=[
                int(value)
                for value in evaluation_payload.get("k_values", [5, 10, 25])
                if int(value) > 0
            ],
            contamination=float(evaluation_payload.get("contamination", 0.12)),
            use_ranker=bool(evaluation_payload.get("use_ranker", True)),
            output_dir=str(evaluation_payload.get("output_dir", "reports/evaluation")),
        ),
        publish=PublishRefreshConfig(
            enabled=bool(publish_payload.get("enabled", True)),
            output_dir=str(publish_payload.get("output_dir", "data/publish/current")),
            max_events=int(publish_payload.get("max_events", 250)),
            public_safe_mode=bool(publish_payload.get("public_safe_mode", True)),
            public_delay_minutes=int(publish_payload.get("public_delay_minutes", 1440)),
            s3_enabled=bool(publish_payload.get("s3_enabled", False)),
            s3_bucket=_none_if_blank(publish_payload.get("s3_bucket")),
            s3_bucket_env=str(publish_payload.get("s3_bucket_env", "PUBLISH_S3_BUCKET")),
            s3_prefix=str(publish_payload.get("s3_prefix", "current")),
            s3_region=_none_if_blank(publish_payload.get("s3_region")),
            s3_region_env=str(publish_payload.get("s3_region_env", "PUBLISH_S3_REGION")),
            s3_endpoint_url=_none_if_blank(publish_payload.get("s3_endpoint_url")),
            s3_endpoint_url_env=str(
                publish_payload.get("s3_endpoint_url_env", "PUBLISH_S3_ENDPOINT_URL")
            ),
            s3_access_key_env=str(publish_payload.get("s3_access_key_env", "AWS_ACCESS_KEY_ID")),
            s3_secret_key_env=str(publish_payload.get("s3_secret_key_env", "AWS_SECRET_ACCESS_KEY")),
            s3_session_token_env=str(publish_payload.get("s3_session_token_env", "AWS_SESSION_TOKEN")),
        ),
    )


def resolve_refresh_steps(mode: str, explicit_steps: list[str] | None = None) -> list[RefreshStep]:
    if explicit_steps:
        normalized = [step.strip().lower() for step in explicit_steps if step.strip()]
        invalid = sorted(set(normalized) - VALID_REFRESH_STEPS)
        if invalid:
            raise ValueError(f"Unknown refresh step(s): {', '.join(invalid)}")
        return normalized

    if mode == "full":
        return FULL_REFRESH_STEPS.copy()
    if mode == "intraday":
        return INTRADAY_REFRESH_STEPS.copy()
    raise ValueError(f"Unknown refresh mode: {mode}")


def run_refresh_pipeline(
    *,
    config: RefreshPipelineConfig,
    steps: list[RefreshStep],
    cli_module,
    paths,
) -> list[str]:
    paths.ensure_directories()
    db.init_database(db_path=paths.db_path, schema_dir=paths.sql_dir)

    completed: list[str] = []
    skipped: list[dict[str, str]] = []
    refresh_run_id = db.begin_ingestion_run(
        db_path=paths.db_path,
        pipeline_name="refresh_pipeline",
        metadata={
            "requested_steps": steps,
            "tickers": config.tickers,
        },
    )
    try:
        for step in steps:
            if step == "sec_reference":
                _run_step(
                    cli_module.cmd_ingest_sec_reference,
                    Namespace(
                        user_agent=_resolve_required_value(config.sec.user_agent, config.sec.user_agent_env),
                        skip_db=False,
                        parent_run_id=refresh_run_id,
                    ),
                )
            elif step == "sec_filings":
                _run_step(
                    cli_module.cmd_ingest_sec_filings,
                    Namespace(
                        user_agent=_resolve_required_value(config.sec.user_agent, config.sec.user_agent_env),
                        tickers=config.tickers,
                        forms=config.sec.forms,
                        per_ticker_limit=config.sec.per_ticker_limit,
                        refresh_reference=config.sec.refresh_reference,
                        skip_db=False,
                        parent_run_id=refresh_run_id,
                    ),
                )
            elif step == "market_daily":
                if not config.market_daily.enabled:
                    continue
                _run_step(
                    cli_module.cmd_ingest_market_daily,
                    Namespace(
                        csv=None,
                        provider=config.market.provider,
                        tickers=config.tickers,
                        source="scheduled_refresh",
                        api_key=config.market.api_key,
                        api_key_env=config.market.api_key_env,
                        outputsize=config.market_daily.outputsize,
                        timeout_seconds=config.market.timeout_seconds,
                        parent_run_id=refresh_run_id,
                    ),
                )
            elif step == "press_releases":
                if not config.issuer_releases.enabled:
                    continue
                _run_step(
                    cli_module.cmd_ingest_press_releases,
                    Namespace(
                        config=_resolve_required_path(
                            config.issuer_releases.config_path,
                            config.issuer_releases.config_path_env,
                            paths.root,
                        ),
                        tickers=config.tickers,
                        per_feed_limit=config.issuer_releases.per_feed_limit,
                        user_agent=_resolve_with_default(
                            config.issuer_releases.user_agent,
                            config.issuer_releases.user_agent_env,
                            "PreNewsTradingSurveillance/0.1",
                        ),
                        timeout_seconds=config.market.timeout_seconds,
                        skip_db=False,
                        parent_run_id=refresh_run_id,
                    ),
                )
            elif step == "market_minute":
                if not config.market_minute.enabled:
                    continue
                _run_step(
                    cli_module.cmd_ingest_market_minute,
                    Namespace(
                        csv=None,
                        provider=config.market.provider,
                        tickers=config.tickers,
                        source="scheduled_refresh",
                        api_key=config.market.api_key,
                        api_key_env=config.market.api_key_env,
                        interval=config.market_minute.interval,
                        outputsize=config.market_minute.outputsize,
                        month=config.market_minute.month,
                        entitlement=config.market_minute.entitlement,
                        adjusted=config.market_minute.adjusted,
                        extended_hours=config.market_minute.extended_hours,
                        timeout_seconds=config.market.timeout_seconds,
                        parent_run_id=refresh_run_id,
                    ),
                )
            elif step == "build_events":
                _run_step(
                    cli_module.cmd_build_sec_events,
                    Namespace(
                        forms=config.sec.forms,
                        sentiment_backend=config.nlp.sentiment_backend,
                        sentiment_model=config.nlp.sentiment_model,
                        novelty_backend=config.nlp.novelty_backend,
                        novelty_model=config.nlp.novelty_model,
                        parent_run_id=refresh_run_id,
                    ),
                )
            elif step == "compute_daily":
                _run_step(
                    cli_module.cmd_compute_daily_features,
                    Namespace(ticker=None, parent_run_id=refresh_run_id),
                )
            elif step == "compute_minute":
                _run_step(
                    cli_module.cmd_compute_minute_features,
                    Namespace(ticker=None, parent_run_id=refresh_run_id),
                )
            elif step == "train_model":
                if not config.model.enabled:
                    skipped.append({"step": step, "reason": "model training disabled"})
                    continue
                training_sample_count = len(db.load_scoring_event_details(paths.db_path))
                if training_sample_count < config.model.min_samples:
                    skipped.append(
                        {
                            "step": step,
                            "reason": (
                                f"insufficient scored events for training: {training_sample_count} < "
                                f"{config.model.min_samples}"
                            ),
                        }
                    )
                    continue
                _run_step(
                    cli_module.cmd_train_model_stack,
                    Namespace(
                        ticker=None,
                        output_dir=_resolve_project_path(config.model.output_dir, paths.root),
                        contamination=config.model.contamination,
                        min_samples=config.model.min_samples,
                        use_ranker=config.model.use_ranker,
                        review_status=config.model.review_status,
                        benchmark_labels=config.model.benchmark_labels,
                        reviewer=config.model.reviewer,
                        parent_run_id=refresh_run_id,
                    ),
                )
            elif step == "score":
                _run_step(
                    cli_module.cmd_score_events,
                    Namespace(
                        ticker=None,
                        engine="auto",
                        model_dir=_resolve_project_path(config.model.output_dir, paths.root),
                        parent_run_id=refresh_run_id,
                    ),
                )
            elif step == "backtest":
                if not config.evaluation.enabled:
                    skipped.append({"step": step, "reason": "evaluation disabled"})
                    continue
                reviewed_count = len(
                    db.load_benchmark_event_details(
                        paths.db_path,
                        review_status=config.evaluation.review_status,
                        benchmark_labels=config.evaluation.benchmark_labels,
                        reviewer=config.evaluation.reviewer,
                    )
                )
                minimum_required = config.evaluation.min_train_size + config.evaluation.folds
                if reviewed_count < minimum_required:
                    skipped.append(
                        {
                            "step": step,
                            "reason": (
                                f"insufficient reviewed benchmark events: {reviewed_count} < "
                                f"{minimum_required}"
                            ),
                        }
                    )
                    continue
                _run_step(
                    cli_module.cmd_run_backtest,
                    Namespace(
                        review_status=config.evaluation.review_status,
                        benchmark_labels=config.evaluation.benchmark_labels,
                        reviewer=config.evaluation.reviewer,
                        folds=config.evaluation.folds,
                        min_train_size=config.evaluation.min_train_size,
                        k_values=config.evaluation.k_values,
                        contamination=config.evaluation.contamination,
                        use_ranker=config.evaluation.use_ranker,
                        output_dir=_resolve_project_path(config.evaluation.output_dir, paths.root),
                        parent_run_id=refresh_run_id,
                    ),
                )
            elif step == "publish":
                if not config.publish.enabled:
                    skipped.append({"step": step, "reason": "publish disabled"})
                    continue
                _run_step(
                    cli_module.cmd_publish_snapshot,
                    Namespace(
                        output_dir=_resolve_publish_output_dir(config.publish.output_dir, paths.root),
                        events_limit=config.publish.max_events,
                        public_safe=config.publish.public_safe_mode,
                        public_delay_minutes=config.publish.public_delay_minutes,
                        s3_bucket=_resolve_optional_value(
                            config.publish.s3_bucket,
                            config.publish.s3_bucket_env,
                        )
                        if config.publish.s3_enabled
                        else None,
                        s3_prefix=config.publish.s3_prefix,
                        s3_region=_resolve_optional_value(
                            config.publish.s3_region,
                            config.publish.s3_region_env,
                        ),
                        s3_endpoint_url=_resolve_optional_value(
                            config.publish.s3_endpoint_url,
                            config.publish.s3_endpoint_url_env,
                        ),
                        s3_access_key_env=config.publish.s3_access_key_env,
                        s3_secret_key_env=config.publish.s3_secret_key_env,
                        s3_session_token_env=config.publish.s3_session_token_env,
                        parent_run_id=refresh_run_id,
                    ),
                )
            else:
                raise ValueError(f"Unhandled refresh step: {step}")
            completed.append(step)
    except Exception as exc:
        db.finish_ingestion_run(
            db_path=paths.db_path,
            run_id=refresh_run_id,
            status="failed",
            row_count=len(completed),
            metadata={
                "completed_steps": completed,
                "skipped_steps": skipped,
                "requested_steps": steps,
                "tickers": config.tickers,
            },
            error_message=str(exc),
        )
        raise

    db.finish_ingestion_run(
        db_path=paths.db_path,
        run_id=refresh_run_id,
        status="success",
        row_count=len(completed),
        metadata={
            "completed_steps": completed,
            "skipped_steps": skipped,
            "requested_steps": steps,
            "tickers": config.tickers,
        },
    )
    return completed


def _run_step(handler: Callable[[Namespace], int], args: Namespace) -> None:
    result = handler(args)
    if result != 0:
        raise RuntimeError(f"Step returned non-zero exit code: {result}")


def _resolve_required_value(explicit: str | None, env_name: str) -> str:
    if explicit:
        return explicit
    env_value = os.getenv(env_name)
    if env_value:
        return env_value
    raise RuntimeError(f"Missing required configuration value. Set `{env_name}` or provide it in config.")


def _resolve_required_path(explicit: str | None, env_name: str, root: Path) -> Path:
    value = _resolve_required_value(explicit, env_name)
    path = Path(value)
    if not path.is_absolute():
        path = root / path
    return path


def _resolve_with_default(explicit: str | None, env_name: str, default: str) -> str:
    resolved = _resolve_optional_value(explicit, env_name)
    return resolved or default


def _none_if_blank(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_publish_output_dir(output_dir: str, root: Path) -> Path:
    return _resolve_project_path(output_dir, root)


def _resolve_project_path(path_value: str, root: Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return root / path


def _resolve_optional_value(explicit: str | None, env_name: str) -> str | None:
    if explicit:
        return explicit
    env_value = os.getenv(env_name)
    if env_value:
        return env_value
    return None
