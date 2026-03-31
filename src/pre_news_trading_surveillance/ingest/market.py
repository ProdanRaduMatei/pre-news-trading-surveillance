from __future__ import annotations

import csv
import json
import os
import ssl
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from ..domain import MarketBarDaily, MarketBarMinute

ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
ALPHA_VANTAGE_ENV_VAR = "ALPHAVANTAGE_API_KEY"
ALPHA_VANTAGE_PROVIDER = "alpha_vantage"
ALPHA_VANTAGE_TZ = ZoneInfo("America/New_York")
DEFAULT_USER_AGENT = "PreNewsTradingSurveillance/0.1"


class MarketProviderError(RuntimeError):
    """Raised when a market data provider request fails or returns an invalid payload."""


def resolve_api_key(api_key: str | None, api_key_env: str = ALPHA_VANTAGE_ENV_VAR) -> str:
    if api_key:
        return api_key
    env_value = os.getenv(api_key_env)
    if env_value:
        return env_value
    raise MarketProviderError(
        f"Missing API key. Set `{api_key_env}` or pass `--api-key` explicitly."
    )


def fetch_alpha_vantage_daily_csv(
    symbol: str,
    api_key: str,
    *,
    outputsize: str = "compact",
    timeout_seconds: int = 30,
    user_agent: str = DEFAULT_USER_AGENT,
) -> str:
    return _fetch_alpha_vantage_csv(
        {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol.upper(),
            "outputsize": outputsize,
            "datatype": "csv",
            "apikey": api_key,
        },
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
    )


def fetch_alpha_vantage_intraday_csv(
    symbol: str,
    api_key: str,
    *,
    interval: str = "1min",
    adjusted: bool = False,
    extended_hours: bool = True,
    outputsize: str = "compact",
    month: str | None = None,
    entitlement: str | None = None,
    timeout_seconds: int = 30,
    user_agent: str = DEFAULT_USER_AGENT,
) -> str:
    params: dict[str, str] = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol.upper(),
        "interval": interval,
        "adjusted": str(adjusted).lower(),
        "extended_hours": str(extended_hours).lower(),
        "outputsize": outputsize,
        "datatype": "csv",
        "apikey": api_key,
    }
    if month:
        params["month"] = month
    if entitlement:
        params["entitlement"] = entitlement
    return _fetch_alpha_vantage_csv(
        params,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
    )


def parse_alpha_vantage_daily_csv(
    text: str,
    *,
    symbol: str,
    source: str,
    ingested_at: str | None = None,
) -> list[MarketBarDaily]:
    rows = _read_csv_rows(text)
    collected_at = ingested_at or _utc_now_iso()
    bars = []
    for row in rows:
        trading_date = _normalize_date(row["timestamp"])
        bars.append(
            MarketBarDaily(
                bar_id=f"{symbol.upper()}:{trading_date}",
                ticker=symbol.upper(),
                trading_date=trading_date,
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(float(row["volume"])),
                source=source,
                ingested_at=collected_at,
            )
        )
    return bars


def parse_alpha_vantage_intraday_csv(
    text: str,
    *,
    symbol: str,
    source: str,
    ingested_at: str | None = None,
) -> list[MarketBarMinute]:
    rows = _read_csv_rows(text)
    collected_at = ingested_at or _utc_now_iso()
    bars = []
    for row in rows:
        bar_start = _normalize_intraday_timestamp(row["timestamp"])
        bars.append(
            MarketBarMinute(
                bar_id=f"{symbol.upper()}:{bar_start}",
                ticker=symbol.upper(),
                bar_start=bar_start,
                trading_date=bar_start[:10],
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(float(row["volume"])),
                source=source,
                ingested_at=collected_at,
            )
        )
    return bars


def persist_raw_market_snapshot(
    paths,
    *,
    provider: str,
    granularity: str,
    ticker: str,
    text: str,
    descriptor: str,
) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"{ticker.upper()}_{descriptor}_{timestamp}.csv"
    raw_path = paths.raw_dir / "market" / provider / granularity / filename
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(text, encoding="utf-8")
    return raw_path


def _fetch_alpha_vantage_csv(
    params: dict[str, str],
    *,
    timeout_seconds: int,
    user_agent: str,
) -> str:
    url = f"{ALPHA_VANTAGE_BASE_URL}?{urlencode(params)}"
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/csv,application/json",
        },
    )
    context = _build_ssl_context()
    with urlopen(request, timeout=timeout_seconds, context=context) as response:
        text = response.read().decode("utf-8")

    _raise_for_provider_error(text)
    return text


def _read_csv_rows(text: str) -> list[dict[str, str]]:
    handle = StringIO(text)
    reader = csv.DictReader(handle)
    if reader.fieldnames is None:
        raise MarketProviderError("Provider response is missing CSV headers.")

    required = {"timestamp", "open", "high", "low", "close", "volume"}
    fieldnames = {field.strip().lower() for field in reader.fieldnames}
    missing = sorted(required - fieldnames)
    if missing:
        raise MarketProviderError(
            f"Provider CSV is missing required columns: {', '.join(missing)}"
        )

    rows = []
    for row in reader:
        normalized = {key.strip().lower(): value.strip() for key, value in row.items() if key is not None}
        rows.append(normalized)
    return rows


def _raise_for_provider_error(text: str) -> None:
    stripped = text.lstrip()
    if not stripped.startswith("{"):
        return

    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        return

    for key in ("Error Message", "Note", "Information", "message"):
        if key in payload:
            raise MarketProviderError(str(payload[key]))

    raise MarketProviderError("Provider returned an unexpected JSON payload instead of CSV.")


def _normalize_date(text: str) -> str:
    return date.fromisoformat(text.strip()).isoformat()


def _normalize_intraday_timestamp(text: str) -> str:
    parsed = _parse_intraday_timestamp(text)
    return parsed.replace(microsecond=0).astimezone(timezone.utc).isoformat()


def _parse_intraday_timestamp(text: str) -> datetime:
    normalized = text.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                parsed = datetime.strptime(normalized, fmt)
                parsed = parsed.replace(tzinfo=ALPHA_VANTAGE_TZ)
                break
            except ValueError:
                continue
        else:
            raise MarketProviderError(f"Unsupported intraday timestamp format: {text}") from None
    else:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ALPHA_VANTAGE_TZ)

    return parsed.astimezone(timezone.utc)


def _build_ssl_context() -> ssl.SSLContext:
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
