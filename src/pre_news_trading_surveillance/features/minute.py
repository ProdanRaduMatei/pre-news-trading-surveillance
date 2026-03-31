from __future__ import annotations

import csv
import math
from bisect import bisect_left
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev

from ..domain import CanonicalEvent, EventMarketFeatureMinute, MarketBarMinute


def load_market_bars_from_csv(csv_path: Path, source: str = "csv_import") -> list[MarketBarMinute]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("CSV file must contain a header row.")

        field_map = {field.lower(): field for field in reader.fieldnames}
        required = {"ticker", "open", "high", "low", "close", "volume"}
        missing = sorted(required - set(field_map))
        if missing:
            raise ValueError(f"CSV file is missing required columns: {', '.join(missing)}")

        timestamp_field = _resolve_timestamp_field(field_map)
        ingested_at = _utc_now()
        bars = []
        for row in reader:
            ticker = row[field_map["ticker"]].strip().upper()
            bar_start = _normalize_timestamp(row[field_map[timestamp_field]].strip())
            trading_date = bar_start[:10]
            bars.append(
                MarketBarMinute(
                    bar_id=f"{ticker}:{bar_start}",
                    ticker=ticker,
                    bar_start=bar_start,
                    trading_date=trading_date,
                    open=float(row[field_map["open"]]),
                    high=float(row[field_map["high"]]),
                    low=float(row[field_map["low"]]),
                    close=float(row[field_map["close"]]),
                    volume=int(float(row[field_map["volume"]])),
                    source=source,
                    ingested_at=ingested_at,
                )
            )
    return bars


def compute_event_market_features(
    events: list[CanonicalEvent],
    bars: list[MarketBarMinute],
) -> list[EventMarketFeatureMinute]:
    prepared = _prepare_bars(bars)
    computed_at = _utc_now()
    features: list[EventMarketFeatureMinute] = []

    for event in events:
        event_ts = _parse_timestamp(event.first_public_at)
        timestamps, ticker_bars = prepared.get(event.ticker, ([], []))
        cutoff = bisect_left(timestamps, event_ts)
        history = ticker_bars[:cutoff]
        features.append(_compute_single_event_feature(event, history, computed_at))

    return features


def _prepare_bars(
    bars: list[MarketBarMinute],
) -> dict[str, tuple[list[datetime], list[MarketBarMinute]]]:
    bars_by_ticker: dict[str, list[tuple[datetime, MarketBarMinute]]] = defaultdict(list)
    for bar in bars:
        bars_by_ticker[bar.ticker].append((_parse_timestamp(bar.bar_start), bar))

    prepared: dict[str, tuple[list[datetime], list[MarketBarMinute]]] = {}
    for ticker, pairs in bars_by_ticker.items():
        pairs.sort(key=lambda pair: pair[0])
        prepared[ticker] = ([pair[0] for pair in pairs], [pair[1] for pair in pairs])
    return prepared


def _compute_single_event_feature(
    event: CanonicalEvent,
    history: list[MarketBarMinute],
    computed_at: str,
) -> EventMarketFeatureMinute:
    last_bar_at = history[-1].bar_start if history else None
    if len(history) < 2:
        return EventMarketFeatureMinute(
            event_id=event.event_id,
            ticker=event.ticker,
            as_of_timestamp=event.first_public_at,
            pre_15m_return=None,
            pre_60m_return=None,
            pre_240m_return=None,
            volume_z_15m=None,
            volume_z_60m=None,
            realized_vol_60m=None,
            range_pct_60m=None,
            last_bar_at=last_bar_at,
            bars_used=len(history),
            computed_at=computed_at,
        )

    return EventMarketFeatureMinute(
        event_id=event.event_id,
        ticker=event.ticker,
        as_of_timestamp=event.first_public_at,
        pre_15m_return=_window_return(history, 15),
        pre_60m_return=_window_return(history, 60),
        pre_240m_return=_window_return(history, 240),
        volume_z_15m=_block_volume_zscore(history, 15, baseline_blocks=24),
        volume_z_60m=_block_volume_zscore(history, 60, baseline_blocks=16),
        realized_vol_60m=_recent_realized_volatility(history, 60),
        range_pct_60m=_window_range_pct(history, 60),
        last_bar_at=last_bar_at,
        bars_used=len(history),
        computed_at=computed_at,
    )


def _window_return(history: list[MarketBarMinute], window: int) -> float | None:
    if len(history) < window + 1:
        return None
    return _safe_return(history[-1].close, history[-(window + 1)].close)


def _block_volume_zscore(
    history: list[MarketBarMinute],
    window: int,
    baseline_blocks: int,
) -> float | None:
    if len(history) < window * 3:
        return None

    value = sum(bar.volume for bar in history[-window:])
    baseline: list[float] = []
    end = len(history) - window

    while end - window >= 0 and len(baseline) < baseline_blocks:
        start = end - window
        baseline.append(sum(bar.volume for bar in history[start:end]))
        end = start

    return _zscore(value, baseline)


def _recent_realized_volatility(history: list[MarketBarMinute], window: int) -> float | None:
    returns = _recent_returns(history, window)
    if len(returns) < 2:
        return None
    return pstdev(returns)


def _recent_returns(history: list[MarketBarMinute], window: int) -> list[float]:
    recent = history[-(window + 1) :]
    returns = []
    for previous, current in zip(recent, recent[1:]):
        maybe_return = _safe_return(current.close, previous.close)
        if maybe_return is not None:
            returns.append(maybe_return)
    return returns


def _window_range_pct(history: list[MarketBarMinute], window: int) -> float | None:
    if len(history) < window:
        return None
    recent = history[-window:]
    low = min(bar.low for bar in recent)
    high = max(bar.high for bar in recent)
    if math.isclose(low, 0.0):
        return None
    return high / low - 1.0


def _zscore(value: float, baseline: list[float]) -> float | None:
    clean = [item for item in baseline if item is not None]
    if len(clean) < 2:
        return None
    sigma = pstdev(clean)
    if math.isclose(sigma, 0.0):
        return None
    return (value - mean(clean)) / sigma


def _safe_return(current: float, prior: float) -> float | None:
    if math.isclose(prior, 0.0):
        return None
    return current / prior - 1.0


def _resolve_timestamp_field(field_map: dict[str, str]) -> str:
    for candidate in ("timestamp", "datetime", "bar_start", "minute"):
        if candidate in field_map:
            return candidate
    raise ValueError(
        "CSV file must include one of the following timestamp columns: timestamp, datetime, bar_start, minute"
    )


def _normalize_timestamp(text: str) -> str:
    return _parse_timestamp(text).replace(microsecond=0).isoformat()


def _parse_timestamp(text: str) -> datetime:
    normalized = text.strip().replace("Z", "+00:00")

    try:
        value = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                value = datetime.strptime(normalized, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Unsupported timestamp format: {text}") from None

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
