from __future__ import annotations

import csv
import math
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, pstdev

from ..domain import CanonicalEvent, EventMarketFeature, MarketBarDaily


def load_market_bars_from_csv(csv_path: Path, source: str = "csv_import") -> list[MarketBarDaily]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("CSV file must contain a header row.")

        field_map = {field.lower(): field for field in reader.fieldnames}
        required = {"ticker", "date", "open", "high", "low", "close", "volume"}
        missing = sorted(required - set(field_map))
        if missing:
            raise ValueError(f"CSV file is missing required columns: {', '.join(missing)}")

        ingested_at = _utc_now()
        bars = []
        for row in reader:
            ticker = row[field_map["ticker"]].strip().upper()
            trading_date = _normalize_date(row[field_map["date"]].strip())
            bars.append(
                MarketBarDaily(
                    bar_id=f"{ticker}:{trading_date}",
                    ticker=ticker,
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
    bars: list[MarketBarDaily],
) -> list[EventMarketFeature]:
    bars_by_ticker: dict[str, list[MarketBarDaily]] = defaultdict(list)
    for bar in bars:
        bars_by_ticker[bar.ticker].append(bar)
    for ticker_bars in bars_by_ticker.values():
        ticker_bars.sort(key=lambda bar: bar.trading_date)

    computed_at = _utc_now()
    features: list[EventMarketFeature] = []
    for event in events:
        ticker_history = bars_by_ticker.get(event.ticker, [])
        event_date = date.fromisoformat(event.event_date)
        history = [bar for bar in ticker_history if date.fromisoformat(bar.trading_date) < event_date]
        features.append(_compute_single_event_feature(event, history, computed_at))
    return features


def _compute_single_event_feature(
    event: CanonicalEvent,
    history: list[MarketBarDaily],
    computed_at: str,
) -> EventMarketFeature:
    if len(history) < 2:
        return EventMarketFeature(
            event_id=event.event_id,
            ticker=event.ticker,
            as_of_date=event.event_date,
            pre_1d_return=None,
            pre_5d_return=None,
            pre_20d_return=None,
            volume_z_1d=None,
            volume_z_5d=None,
            volatility_20d=None,
            gap_pct=None,
            avg_volume_20d=None,
            bars_used=len(history),
            computed_at=computed_at,
        )

    last_bar = history[-1]
    prev_bar = history[-2]
    pre_1d_return = _safe_return(last_bar.close, prev_bar.close)
    pre_5d_return = _window_return(history, 5)
    pre_20d_return = _window_return(history, 20)

    baseline_volumes = [bar.volume for bar in history[-21:-1]]
    recent_five_volumes = [bar.volume for bar in history[-5:]]
    volume_z_1d = _zscore(last_bar.volume, baseline_volumes)
    volume_z_5d = _zscore(mean(recent_five_volumes), history[-25:-5] and [bar.volume for bar in history[-25:-5]])

    returns_20d = _recent_daily_returns(history, window=20)
    volatility_20d = pstdev(returns_20d) if len(returns_20d) >= 2 else None
    gap_pct = _safe_return(last_bar.open, prev_bar.close)
    avg_volume_20d = mean(baseline_volumes) if baseline_volumes else None

    return EventMarketFeature(
        event_id=event.event_id,
        ticker=event.ticker,
        as_of_date=event.event_date,
        pre_1d_return=pre_1d_return,
        pre_5d_return=pre_5d_return,
        pre_20d_return=pre_20d_return,
        volume_z_1d=volume_z_1d,
        volume_z_5d=volume_z_5d,
        volatility_20d=volatility_20d,
        gap_pct=gap_pct,
        avg_volume_20d=avg_volume_20d,
        bars_used=len(history),
        computed_at=computed_at,
    )


def _window_return(history: list[MarketBarDaily], days: int) -> float | None:
    if len(history) < days + 1:
        return None
    return _safe_return(history[-1].close, history[-(days + 1)].close)


def _recent_daily_returns(history: list[MarketBarDaily], window: int) -> list[float]:
    recent = history[-(window + 1) :]
    returns = []
    for previous, current in zip(recent, recent[1:]):
        maybe_return = _safe_return(current.close, previous.close)
        if maybe_return is not None:
            returns.append(maybe_return)
    return returns


def _safe_return(current: float, prior: float) -> float | None:
    if prior == 0:
        return None
    return current / prior - 1.0


def _zscore(value: float, baseline: list[float]) -> float | None:
    clean = [item for item in baseline if item is not None]
    if len(clean) < 2:
        return None
    sigma = pstdev(clean)
    if math.isclose(sigma, 0.0):
        return None
    return (value - mean(clean)) / sigma


def _normalize_date(text: str) -> str:
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").date().isoformat()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
