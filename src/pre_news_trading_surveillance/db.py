from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from uuid import uuid4

from .domain import (
    BenchmarkLabel,
    CanonicalEvent,
    EventMarketFeature,
    EventMarketFeatureMinute,
    EventScore,
    MarketBarDaily,
    MarketBarMinute,
)
from .ingest.models import RawFilingRecord, RawIssuerReleaseRecord, TickerReference


def _require_duckdb():
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "DuckDB is required for database commands. Install dependencies with `pip install -e .`."
        ) from exc
    return duckdb


def _fetch_rows(
    db_path: Path,
    query: str,
    params: list[object] | None = None,
) -> tuple[list[str], list[tuple[object, ...]]]:
    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        cursor = connection.execute(query, params or [])
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
    finally:
        connection.close()
    return columns, rows


def _fetch_dict_rows(
    db_path: Path,
    query: str,
    params: list[object] | None = None,
) -> list[dict[str, object]]:
    columns, rows = _fetch_rows(db_path, query, params)
    return [_row_to_dict(columns, row) for row in rows]


def init_database(db_path: Path, schema_path: Path | None = None, schema_dir: Path | None = None) -> Path:
    duckdb = _require_duckdb()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(db_path))
    try:
        if schema_dir is not None:
            for file_path in sorted(schema_dir.glob("*.sql")):
                connection.execute(file_path.read_text(encoding="utf-8"))
        elif schema_path is not None:
            connection.execute(schema_path.read_text(encoding="utf-8"))
        else:
            raise ValueError("Either schema_path or schema_dir must be provided.")
    finally:
        connection.close()
    return db_path


def upsert_ticker_references(db_path: Path, references: list[TickerReference]) -> int:
    if not references:
        return 0

    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        connection.begin()
        connection.executemany(
            "DELETE FROM raw_ticker_reference WHERE ticker = ?",
            [(reference.ticker,) for reference in references],
        )
        connection.executemany(
            """
            INSERT INTO raw_ticker_reference
              (ticker, cik, company_name, source_url, retrieved_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [reference.as_db_row() for reference in references],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(references)


def upsert_raw_filings(db_path: Path, filings: list[RawFilingRecord]) -> int:
    if not filings:
        return 0

    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        connection.begin()
        connection.executemany(
            "DELETE FROM raw_filings WHERE filing_id = ?",
            [(filing.filing_id,) for filing in filings],
        )
        connection.executemany(
            """
            INSERT INTO raw_filings (
              filing_id,
              ticker,
              cik,
              company_name,
              accession_no,
              form_type,
              filing_date,
              accepted_at,
              items_json,
              primary_document,
              primary_doc_description,
              source_url,
              raw_path,
              ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [filing.as_db_row() for filing in filings],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(filings)


def upsert_raw_issuer_releases(db_path: Path, releases: list[RawIssuerReleaseRecord]) -> int:
    if not releases:
        return 0

    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        connection.begin()
        connection.executemany(
            "DELETE FROM raw_issuer_releases WHERE release_id = ?",
            [(release.release_id,) for release in releases],
        )
        connection.executemany(
            """
            INSERT INTO raw_issuer_releases (
              release_id,
              ticker,
              issuer_name,
              source_name,
              feed_url,
              entry_guid,
              title,
              summary_text,
              source_url,
              published_at,
              raw_path,
              ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [release.as_db_row() for release in releases],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(releases)


def upsert_market_bars_daily(db_path: Path, bars: list[MarketBarDaily]) -> int:
    if not bars:
        return 0

    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        connection.begin()
        connection.executemany(
            "DELETE FROM market_bars_daily WHERE bar_id = ?",
            [(bar.bar_id,) for bar in bars],
        )
        connection.executemany(
            """
            INSERT INTO market_bars_daily (
              bar_id,
              ticker,
              trading_date,
              open,
              high,
              low,
              close,
              volume,
              source,
              ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [bar.as_db_row() for bar in bars],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(bars)


def upsert_market_bars_minute(db_path: Path, bars: list[MarketBarMinute]) -> int:
    if not bars:
        return 0

    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        connection.begin()
        connection.executemany(
            "DELETE FROM market_bars_minute WHERE bar_id = ?",
            [(bar.bar_id,) for bar in bars],
        )
        connection.executemany(
            """
            INSERT INTO market_bars_minute (
              bar_id,
              ticker,
              bar_start,
              trading_date,
              open,
              high,
              low,
              close,
              volume,
              source,
              ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [bar.as_db_row() for bar in bars],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(bars)


def upsert_events(db_path: Path, events: list[CanonicalEvent]) -> int:
    if not events:
        return 0

    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        connection.begin()
        connection.executemany(
            "DELETE FROM events WHERE event_id = ?",
            [(event.event_id,) for event in events],
        )
        connection.executemany(
            """
            INSERT INTO events (
              event_id,
              source_event_id,
              source_table,
              ticker,
              issuer_name,
              first_public_at,
              event_date,
              event_type,
              sentiment_label,
              sentiment_score,
              title,
              summary,
              source_url,
              primary_document,
              sec_items_json,
              official_source_flag,
              timestamp_confidence,
              classifier_backend,
              sentiment_backend,
              novelty_backend,
              source_quality,
              novelty,
              impact_score,
              built_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [event.as_db_row() for event in events],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(events)


def upsert_event_market_features(db_path: Path, features: list[EventMarketFeature]) -> int:
    if not features:
        return 0

    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        connection.begin()
        connection.executemany(
            "DELETE FROM event_market_features_daily WHERE event_id = ?",
            [(feature.event_id,) for feature in features],
        )
        connection.executemany(
            """
            INSERT INTO event_market_features_daily (
              event_id,
              ticker,
              as_of_date,
              pre_1d_return,
              pre_5d_return,
              pre_20d_return,
              volume_z_1d,
              volume_z_5d,
              volatility_20d,
              gap_pct,
              avg_volume_20d,
              bars_used,
              computed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [feature.as_db_row() for feature in features],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(features)


def upsert_event_market_features_minute(
    db_path: Path,
    features: list[EventMarketFeatureMinute],
) -> int:
    if not features:
        return 0

    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        connection.begin()
        connection.executemany(
            "DELETE FROM event_market_features_minute WHERE event_id = ?",
            [(feature.event_id,) for feature in features],
        )
        connection.executemany(
            """
            INSERT INTO event_market_features_minute (
              event_id,
              ticker,
              as_of_timestamp,
              pre_15m_return,
              pre_60m_return,
              pre_240m_return,
              volume_z_15m,
              volume_z_60m,
              realized_vol_60m,
              range_pct_60m,
              last_bar_at,
              bars_used,
              computed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [feature.as_db_row() for feature in features],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(features)


def upsert_event_scores(db_path: Path, scores: list[EventScore]) -> int:
    if not scores:
        return 0

    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        connection.begin()
        connection.executemany(
            "DELETE FROM event_scores WHERE event_id = ?",
            [(score.event_id,) for score in scores],
        )
        connection.executemany(
            """
            INSERT INTO event_scores (
              event_id,
              rule_score,
              suspiciousness_score,
              score_band,
              directional_alignment,
              explanation_payload,
              scored_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [score.as_db_row() for score in scores],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(scores)


def upsert_benchmark_labels(db_path: Path, labels: list[BenchmarkLabel]) -> int:
    if not labels:
        return 0

    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        connection.begin()
        connection.executemany(
            "DELETE FROM benchmark_event_labels WHERE event_id = ?",
            [(label.event_id,) for label in labels],
        )
        connection.executemany(
            """
            INSERT INTO benchmark_event_labels (
              event_id,
              benchmark_label,
              review_status,
              reviewer,
              label_source,
              confidence,
              review_notes,
              metadata_json,
              created_at,
              updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [label.as_db_row() for label in labels],
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return len(labels)


def load_raw_filings(
    db_path: Path,
    forms: list[str] | None = None,
    limit: int | None = None,
) -> list[RawFilingRecord]:
    duckdb = _require_duckdb()
    query = """
        SELECT
          filing_id,
          ticker,
          cik,
          company_name,
          accession_no,
          form_type,
          CAST(filing_date AS VARCHAR) AS filing_date,
          CAST(accepted_at AS VARCHAR) AS accepted_at,
          items_json,
          primary_document,
          primary_doc_description,
          source_url,
          raw_path,
          CAST(ingested_at AS VARCHAR) AS ingested_at
        FROM raw_filings
        WHERE 1 = 1
    """
    params: list[object] = []
    if forms:
        placeholders = ", ".join(["?"] * len(forms))
        query += f" AND UPPER(form_type) IN ({placeholders})"
        params.extend([form.upper() for form in forms])
    query += " ORDER BY COALESCE(accepted_at, CAST(filing_date AS TIMESTAMP), ingested_at) ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    connection = duckdb.connect(str(db_path))
    try:
        rows = connection.execute(query, params).fetchall()
    finally:
        connection.close()

    return [
        RawFilingRecord(
            filing_id=row[0],
            ticker=row[1],
            cik=row[2],
            company_name=row[3],
            accession_no=row[4],
            form_type=row[5],
            filing_date=row[6],
            accepted_at=row[7],
            items_json=row[8],
            primary_document=row[9],
            primary_doc_description=row[10],
            source_url=row[11],
            raw_path=row[12],
            ingested_at=row[13],
        )
        for row in rows
    ]


def load_raw_issuer_releases(
    db_path: Path,
    ticker: str | None = None,
    limit: int | None = None,
) -> list[RawIssuerReleaseRecord]:
    duckdb = _require_duckdb()
    query = """
        SELECT
          release_id,
          ticker,
          issuer_name,
          source_name,
          feed_url,
          entry_guid,
          title,
          summary_text,
          source_url,
          CAST(published_at AS VARCHAR) AS published_at,
          raw_path,
          CAST(ingested_at AS VARCHAR) AS ingested_at
        FROM raw_issuer_releases
        WHERE 1 = 1
    """
    params: list[object] = []
    if ticker:
        query += " AND ticker = ?"
        params.append(ticker.upper())
    query += " ORDER BY COALESCE(published_at, ingested_at) ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    connection = duckdb.connect(str(db_path))
    try:
        rows = connection.execute(query, params).fetchall()
    finally:
        connection.close()

    return [
        RawIssuerReleaseRecord(
            release_id=row[0],
            ticker=row[1],
            issuer_name=row[2],
            source_name=row[3],
            feed_url=row[4],
            entry_guid=row[5],
            title=row[6],
            summary_text=row[7],
            source_url=row[8],
            published_at=row[9],
            raw_path=row[10],
            ingested_at=row[11],
        )
        for row in rows
    ]


def load_events(
    db_path: Path,
    ticker: str | None = None,
    event_type: str | None = None,
    limit: int | None = None,
) -> list[CanonicalEvent]:
    duckdb = _require_duckdb()
    query = """
        SELECT
          event_id,
          source_event_id,
          source_table,
          ticker,
          issuer_name,
          CAST(first_public_at AS VARCHAR) AS first_public_at,
          CAST(event_date AS VARCHAR) AS event_date,
          event_type,
          sentiment_label,
          sentiment_score,
          title,
          summary,
          source_url,
          primary_document,
          sec_items_json,
          official_source_flag,
          timestamp_confidence,
          classifier_backend,
          sentiment_backend,
          novelty_backend,
          source_quality,
          novelty,
          impact_score,
          CAST(built_at AS VARCHAR) AS built_at
        FROM events
        WHERE 1 = 1
    """
    params: list[object] = []
    if ticker:
        query += " AND ticker = ?"
        params.append(ticker.upper())
    if event_type:
        query += " AND event_type = ?"
        params.append(event_type)
    query += " ORDER BY first_public_at DESC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    connection = duckdb.connect(str(db_path))
    try:
        rows = connection.execute(query, params).fetchall()
    finally:
        connection.close()

    return [
        CanonicalEvent(
            event_id=row[0],
            source_event_id=row[1],
            source_table=row[2],
            ticker=row[3],
            issuer_name=row[4],
            first_public_at=row[5],
            event_date=row[6],
            event_type=row[7],
            sentiment_label=row[8],
            sentiment_score=float(row[9]),
            title=row[10],
            summary=row[11],
            source_url=row[12],
            primary_document=row[13],
            sec_items_json=row[14],
            official_source_flag=bool(row[15]),
            timestamp_confidence=row[16],
            classifier_backend=row[17],
            sentiment_backend=row[18],
            novelty_backend=row[19],
            source_quality=float(row[20]),
            novelty=float(row[21]),
            impact_score=float(row[22]),
            built_at=row[23],
        )
        for row in rows
    ]


def load_market_bars_daily(db_path: Path, ticker: str | None = None) -> list[MarketBarDaily]:
    duckdb = _require_duckdb()
    query = """
        SELECT
          bar_id,
          ticker,
          CAST(trading_date AS VARCHAR) AS trading_date,
          open,
          high,
          low,
          close,
          volume,
          source,
          CAST(ingested_at AS VARCHAR) AS ingested_at
        FROM market_bars_daily
        WHERE 1 = 1
    """
    params: list[object] = []
    if ticker:
        query += " AND ticker = ?"
        params.append(ticker.upper())
    query += " ORDER BY ticker, trading_date ASC"

    connection = duckdb.connect(str(db_path))
    try:
        rows = connection.execute(query, params).fetchall()
    finally:
        connection.close()

    return [
        MarketBarDaily(
            bar_id=row[0],
            ticker=row[1],
            trading_date=row[2],
            open=float(row[3]),
            high=float(row[4]),
            low=float(row[5]),
            close=float(row[6]),
            volume=int(row[7]),
            source=row[8],
            ingested_at=row[9],
        )
        for row in rows
    ]


def load_market_bars_minute(db_path: Path, ticker: str | None = None) -> list[MarketBarMinute]:
    duckdb = _require_duckdb()
    query = """
        SELECT
          bar_id,
          ticker,
          CAST(bar_start AS VARCHAR) AS bar_start,
          CAST(trading_date AS VARCHAR) AS trading_date,
          open,
          high,
          low,
          close,
          volume,
          source,
          CAST(ingested_at AS VARCHAR) AS ingested_at
        FROM market_bars_minute
        WHERE 1 = 1
    """
    params: list[object] = []
    if ticker:
        query += " AND ticker = ?"
        params.append(ticker.upper())
    query += " ORDER BY ticker, bar_start ASC"

    connection = duckdb.connect(str(db_path))
    try:
        rows = connection.execute(query, params).fetchall()
    finally:
        connection.close()

    return [
        MarketBarMinute(
            bar_id=row[0],
            ticker=row[1],
            bar_start=row[2],
            trading_date=row[3],
            open=float(row[4]),
            high=float(row[5]),
            low=float(row[6]),
            close=float(row[7]),
            volume=int(row[8]),
            source=row[9],
            ingested_at=row[10],
        )
        for row in rows
    ]


def list_ranked_events(
    db_path: Path,
    limit: int = 25,
    offset: int = 0,
    ticker: str | None = None,
    event_type: str | None = None,
    min_score: float | None = None,
    max_first_public_at: str | None = None,
) -> list[dict[str, object]]:
    duckdb = _require_duckdb()
    query = """
        SELECT
          e.event_id,
          e.source_table,
          e.ticker,
          e.issuer_name,
          CAST(e.first_public_at AS VARCHAR) AS first_public_at,
          e.event_type,
          e.sentiment_label,
          e.title,
          e.summary,
          e.source_url,
          e.timestamp_confidence,
          e.classifier_backend,
          e.sentiment_backend,
          e.novelty_backend,
          e.novelty,
          e.impact_score,
          f.pre_1d_return,
          f.pre_5d_return,
          f.volume_z_1d,
          f.volume_z_5d,
          fm.pre_15m_return,
          fm.pre_60m_return,
          fm.volume_z_15m,
          fm.volume_z_60m,
          s.suspiciousness_score,
          s.score_band,
          s.explanation_payload
        FROM events e
        LEFT JOIN event_market_features_daily f ON e.event_id = f.event_id
        LEFT JOIN event_market_features_minute fm ON e.event_id = fm.event_id
        LEFT JOIN event_scores s ON e.event_id = s.event_id
        WHERE 1 = 1
    """
    params: list[object] = []
    if ticker:
        query += " AND e.ticker = ?"
        params.append(ticker.upper())
    if event_type:
        query += " AND e.event_type = ?"
        params.append(event_type)
    if min_score is not None:
        query += " AND COALESCE(s.suspiciousness_score, 0) >= ?"
        params.append(min_score)
    if max_first_public_at:
        query += " AND e.first_public_at <= ?"
        params.append(max_first_public_at)

    query += " ORDER BY COALESCE(s.suspiciousness_score, 0) DESC, e.first_public_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    return _fetch_dict_rows(db_path, query, params)


def count_ranked_events(
    db_path: Path,
    *,
    ticker: str | None = None,
    event_type: str | None = None,
    min_score: float | None = None,
    max_first_public_at: str | None = None,
) -> int:
    query = """
        SELECT COUNT(*)
        FROM events e
        LEFT JOIN event_scores s ON e.event_id = s.event_id
        WHERE 1 = 1
    """
    params: list[object] = []
    if ticker:
        query += " AND e.ticker = ?"
        params.append(ticker.upper())
    if event_type:
        query += " AND e.event_type = ?"
        params.append(event_type)
    if min_score is not None:
        query += " AND COALESCE(s.suspiciousness_score, 0) >= ?"
        params.append(min_score)
    if max_first_public_at:
        query += " AND e.first_public_at <= ?"
        params.append(max_first_public_at)
    _columns, rows = _fetch_rows(db_path, query, params)
    return int(rows[0][0]) if rows else 0


def get_ranked_event(
    db_path: Path,
    event_id: str,
    *,
    max_first_public_at: str | None = None,
) -> dict[str, object] | None:
    query = """
        SELECT
          e.event_id,
          e.source_event_id,
          e.source_table,
          e.ticker,
          e.issuer_name,
          CAST(e.first_public_at AS VARCHAR) AS first_public_at,
          CAST(e.event_date AS VARCHAR) AS event_date,
          e.event_type,
          e.sentiment_label,
          e.sentiment_score,
          e.title,
          e.summary,
          e.source_url,
          e.primary_document,
          e.sec_items_json,
          e.official_source_flag,
          e.timestamp_confidence,
          e.classifier_backend,
          e.sentiment_backend,
          e.novelty_backend,
          e.source_quality,
          e.novelty,
          e.impact_score,
          CAST(e.built_at AS VARCHAR) AS built_at,
          f.pre_1d_return,
          f.pre_5d_return,
          f.pre_20d_return,
          CAST(f.as_of_date AS VARCHAR) AS as_of_date,
          f.volume_z_1d,
          f.volume_z_5d,
          f.volatility_20d,
          f.gap_pct,
          f.avg_volume_20d,
          f.bars_used,
          fm.pre_15m_return,
          fm.pre_60m_return,
          fm.pre_240m_return,
          CAST(fm.as_of_timestamp AS VARCHAR) AS minute_as_of_timestamp,
          fm.volume_z_15m,
          fm.volume_z_60m,
          fm.realized_vol_60m,
          fm.range_pct_60m,
          CAST(fm.last_bar_at AS VARCHAR) AS last_bar_at,
          fm.bars_used AS minute_bars_used,
          s.rule_score,
          s.suspiciousness_score,
          s.score_band,
          s.directional_alignment,
          s.explanation_payload,
          CAST(s.scored_at AS VARCHAR) AS scored_at
        FROM events e
        LEFT JOIN event_market_features_daily f ON e.event_id = f.event_id
        LEFT JOIN event_market_features_minute fm ON e.event_id = fm.event_id
        LEFT JOIN event_scores s ON e.event_id = s.event_id
        WHERE e.event_id = ?
    """
    params: list[object] = [event_id]
    if max_first_public_at:
        query += " AND e.first_public_at <= ?"
        params.append(max_first_public_at)
    columns, rows = _fetch_rows(db_path, query, params)
    if not rows:
        return None
    return _row_to_dict(columns, rows[0])


def load_scoring_event_details(
    db_path: Path,
    *,
    ticker: str | None = None,
    limit: int | None = None,
) -> list[dict[str, object]]:
    query = """
        SELECT
          e.event_id,
          e.source_event_id,
          e.source_table,
          e.ticker,
          e.issuer_name,
          CAST(e.first_public_at AS VARCHAR) AS first_public_at,
          CAST(e.event_date AS VARCHAR) AS event_date,
          e.event_type,
          e.sentiment_label,
          e.sentiment_score,
          e.title,
          e.summary,
          e.source_url,
          e.primary_document,
          e.sec_items_json,
          e.official_source_flag,
          e.timestamp_confidence,
          e.classifier_backend,
          e.sentiment_backend,
          e.novelty_backend,
          e.source_quality,
          e.novelty,
          e.impact_score,
          CAST(e.built_at AS VARCHAR) AS built_at,
          f.pre_1d_return,
          f.pre_5d_return,
          f.pre_20d_return,
          CAST(f.as_of_date AS VARCHAR) AS as_of_date,
          f.volume_z_1d,
          f.volume_z_5d,
          f.volatility_20d,
          f.gap_pct,
          f.avg_volume_20d,
          f.bars_used,
          fm.pre_15m_return,
          fm.pre_60m_return,
          fm.pre_240m_return,
          CAST(fm.as_of_timestamp AS VARCHAR) AS minute_as_of_timestamp,
          fm.volume_z_15m,
          fm.volume_z_60m,
          fm.realized_vol_60m,
          fm.range_pct_60m,
          CAST(fm.last_bar_at AS VARCHAR) AS last_bar_at,
          fm.bars_used AS minute_bars_used,
          s.rule_score,
          s.suspiciousness_score,
          s.score_band,
          s.directional_alignment,
          s.explanation_payload,
          CAST(s.scored_at AS VARCHAR) AS scored_at
        FROM events e
        LEFT JOIN event_market_features_daily f ON e.event_id = f.event_id
        LEFT JOIN event_market_features_minute fm ON e.event_id = fm.event_id
        LEFT JOIN event_scores s ON e.event_id = s.event_id
        WHERE 1 = 1
    """
    params: list[object] = []
    if ticker:
        query += " AND e.ticker = ?"
        params.append(ticker.upper())
    query += " ORDER BY e.first_public_at DESC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return _fetch_dict_rows(db_path, query, params)


def load_benchmark_event_details(
    db_path: Path,
    *,
    review_status: str = "reviewed",
    benchmark_labels: list[str] | None = None,
    reviewer: str | None = None,
    limit: int | None = None,
) -> list[dict[str, object]]:
    query = """
        SELECT
          e.event_id,
          e.source_event_id,
          e.source_table,
          e.ticker,
          e.issuer_name,
          CAST(e.first_public_at AS VARCHAR) AS first_public_at,
          CAST(e.event_date AS VARCHAR) AS event_date,
          e.event_type,
          e.sentiment_label,
          e.sentiment_score,
          e.title,
          e.summary,
          e.source_url,
          e.primary_document,
          e.sec_items_json,
          e.official_source_flag,
          e.timestamp_confidence,
          e.classifier_backend,
          e.sentiment_backend,
          e.novelty_backend,
          e.source_quality,
          e.novelty,
          e.impact_score,
          CAST(e.built_at AS VARCHAR) AS built_at,
          f.pre_1d_return,
          f.pre_5d_return,
          f.pre_20d_return,
          CAST(f.as_of_date AS VARCHAR) AS as_of_date,
          f.volume_z_1d,
          f.volume_z_5d,
          f.volatility_20d,
          f.gap_pct,
          f.avg_volume_20d,
          f.bars_used,
          fm.pre_15m_return,
          fm.pre_60m_return,
          fm.pre_240m_return,
          CAST(fm.as_of_timestamp AS VARCHAR) AS minute_as_of_timestamp,
          fm.volume_z_15m,
          fm.volume_z_60m,
          fm.realized_vol_60m,
          fm.range_pct_60m,
          CAST(fm.last_bar_at AS VARCHAR) AS last_bar_at,
          fm.bars_used AS minute_bars_used,
          s.rule_score,
          s.suspiciousness_score,
          s.score_band,
          s.directional_alignment,
          s.explanation_payload,
          CAST(s.scored_at AS VARCHAR) AS scored_at,
          b.benchmark_label,
          b.review_status,
          b.reviewer,
          b.label_source,
          b.confidence,
          b.review_notes,
          b.metadata_json,
          CAST(b.created_at AS VARCHAR) AS benchmark_created_at,
          CAST(b.updated_at AS VARCHAR) AS benchmark_updated_at
        FROM benchmark_event_labels b
        INNER JOIN events e ON b.event_id = e.event_id
        LEFT JOIN event_market_features_daily f ON e.event_id = f.event_id
        LEFT JOIN event_market_features_minute fm ON e.event_id = fm.event_id
        LEFT JOIN event_scores s ON e.event_id = s.event_id
        WHERE 1 = 1
    """
    params: list[object] = []
    if review_status:
        query += " AND b.review_status = ?"
        params.append(review_status)
    if reviewer:
        query += " AND b.reviewer = ?"
        params.append(reviewer)
    if benchmark_labels:
        placeholders = ", ".join("?" for _ in benchmark_labels)
        query += f" AND b.benchmark_label IN ({placeholders})"
        params.extend(benchmark_labels)

    query += " ORDER BY e.first_public_at ASC, e.event_id ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
    return _fetch_dict_rows(db_path, query, params)


def list_benchmark_labels(
    db_path: Path,
    *,
    limit: int = 100,
    review_status: str | None = None,
    benchmark_label: str | None = None,
) -> list[dict[str, object]]:
    filters: list[str] = []
    params: list[object] = []
    if review_status:
        filters.append("review_status = ?")
        params.append(review_status)
    if benchmark_label:
        filters.append("benchmark_label = ?")
        params.append(benchmark_label)

    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.append(limit)
    return _fetch_dict_rows(
        db_path,
        f"""
        SELECT
          event_id,
          benchmark_label,
          review_status,
          reviewer,
          label_source,
          confidence,
          review_notes,
          metadata_json,
          CAST(created_at AS VARCHAR) AS created_at,
          CAST(updated_at AS VARCHAR) AS updated_at
        FROM benchmark_event_labels
        {where_sql}
        ORDER BY updated_at DESC, event_id ASC
        LIMIT ?
        """,
        params,
    )


def get_dashboard_summary(db_path: Path, *, max_first_public_at: str | None = None) -> dict[str, object]:
    where_clause = "WHERE 1 = 1"
    params: list[object] = []
    if max_first_public_at:
        where_clause += " AND e.first_public_at <= ?"
        params.append(max_first_public_at)

    overview = _fetch_dict_rows(
        db_path,
        f"""
        SELECT
          COUNT(*) AS total_events,
          COUNT(DISTINCT e.ticker) AS tracked_tickers,
          CAST(MIN(e.first_public_at) AS VARCHAR) AS coverage_start,
          CAST(MAX(e.first_public_at) AS VARCHAR) AS coverage_end,
          ROUND(COALESCE(AVG(s.suspiciousness_score), 0), 2) AS average_score,
          ROUND(COALESCE(MAX(s.suspiciousness_score), 0), 2) AS peak_score,
          COALESCE(SUM(CASE WHEN s.score_band = 'High' THEN 1 ELSE 0 END), 0) AS high_risk_events,
          COALESCE(SUM(CASE WHEN s.score_band = 'Medium' THEN 1 ELSE 0 END), 0) AS medium_risk_events,
          COALESCE(SUM(CASE WHEN s.score_band = 'Low' THEN 1 ELSE 0 END), 0) AS low_risk_events,
          CAST(MAX(e.built_at) AS VARCHAR) AS last_built_at,
          CAST(MAX(s.scored_at) AS VARCHAR) AS last_scored_at
        FROM events e
        LEFT JOIN event_scores s ON e.event_id = s.event_id
        {where_clause}
        """,
        params,
    )[0]

    score_bands = _fetch_dict_rows(
        db_path,
        f"""
        SELECT
          COALESCE(s.score_band, 'Unscored') AS score_band,
          COUNT(*) AS event_count,
          ROUND(COALESCE(AVG(s.suspiciousness_score), 0), 2) AS average_score
        FROM events e
        LEFT JOIN event_scores s ON e.event_id = s.event_id
        {where_clause}
        GROUP BY 1
        ORDER BY CASE COALESCE(s.score_band, 'Unscored')
          WHEN 'High' THEN 1
          WHEN 'Medium' THEN 2
          WHEN 'Low' THEN 3
          ELSE 4
        END
        """,
        params,
    )

    event_types = _fetch_dict_rows(
        db_path,
        f"""
        SELECT
          e.event_type,
          COUNT(*) AS event_count,
          ROUND(COALESCE(AVG(s.suspiciousness_score), 0), 2) AS average_score,
          ROUND(COALESCE(MAX(s.suspiciousness_score), 0), 2) AS peak_score
        FROM events e
        LEFT JOIN event_scores s ON e.event_id = s.event_id
        {where_clause}
        GROUP BY 1
        ORDER BY event_count DESC, average_score DESC, e.event_type ASC
        LIMIT 8
        """,
        params,
    )

    top_tickers = _fetch_dict_rows(
        db_path,
        f"""
        SELECT
          e.ticker,
          COUNT(*) AS event_count,
          ROUND(COALESCE(AVG(s.suspiciousness_score), 0), 2) AS average_score,
          ROUND(COALESCE(MAX(s.suspiciousness_score), 0), 2) AS peak_score
        FROM events e
        LEFT JOIN event_scores s ON e.event_id = s.event_id
        {where_clause}
        GROUP BY 1
        ORDER BY peak_score DESC, event_count DESC, e.ticker ASC
        LIMIT 8
        """,
        params,
    )

    recent_activity = _fetch_dict_rows(
        db_path,
        f"""
        SELECT
          CAST(DATE_TRUNC('day', e.first_public_at) AS VARCHAR) AS event_day,
          COUNT(*) AS event_count,
          ROUND(COALESCE(AVG(s.suspiciousness_score), 0), 2) AS average_score
        FROM events e
        LEFT JOIN event_scores s ON e.event_id = s.event_id
        {where_clause}
        GROUP BY 1
        ORDER BY event_day DESC
        LIMIT 10
        """,
        params,
    )
    recent_activity.reverse()

    return {
        "overview": overview,
        "score_bands": score_bands,
        "event_types": event_types,
        "top_tickers": top_tickers,
        "recent_activity": recent_activity,
    }


def record_ingestion_run(
    db_path: Path,
    pipeline_name: str,
    status: str,
    row_count: int,
    metadata: dict[str, object] | None = None,
    *,
    error_message: str | None = None,
    artifact_paths: list[str] | None = None,
    parent_run_id: str | None = None,
    attempt_count: int = 0,
) -> str:
    run_id = begin_ingestion_run(
        db_path=db_path,
        pipeline_name=pipeline_name,
        metadata=metadata,
        parent_run_id=parent_run_id,
    )
    finish_ingestion_run(
        db_path=db_path,
        run_id=run_id,
        status=status,
        row_count=row_count,
        metadata=metadata,
        error_message=error_message,
        artifact_paths=artifact_paths,
        attempt_count=attempt_count,
    )
    return run_id


def begin_ingestion_run(
    db_path: Path,
    pipeline_name: str,
    metadata: dict[str, object] | None = None,
    *,
    parent_run_id: str | None = None,
) -> str:
    duckdb = _require_duckdb()
    run_id = str(uuid4())
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute(
            """
            INSERT INTO ingestion_runs (
              run_id,
              pipeline_name,
              status,
              row_count,
              metadata_json,
              started_at,
              artifact_paths_json,
              parent_run_id,
              attempt_count
            )
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?)
            """,
            (
                run_id,
                pipeline_name,
                "running",
                0,
                json.dumps(metadata or {}, sort_keys=True),
                json.dumps([], sort_keys=True),
                parent_run_id,
                0,
            ),
        )
    finally:
        connection.close()
    return run_id


def finish_ingestion_run(
    db_path: Path,
    run_id: str,
    *,
    status: str,
    row_count: int,
    metadata: dict[str, object] | None = None,
    error_message: str | None = None,
    artifact_paths: list[str] | None = None,
    attempt_count: int = 0,
) -> None:
    duckdb = _require_duckdb()
    connection = duckdb.connect(str(db_path))
    try:
        row = connection.execute(
            "SELECT started_at FROM ingestion_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown ingestion run id: {run_id}")

        started_at = _coerce_started_at(row[0])
        finished_at = datetime.now(timezone.utc)
        duration_ms = max(int((finished_at - started_at).total_seconds() * 1000), 0)

        connection.execute(
            """
            UPDATE ingestion_runs
            SET
              status = ?,
              row_count = ?,
              metadata_json = ?,
              finished_at = ?,
              duration_ms = ?,
              error_message = ?,
              artifact_paths_json = ?,
              attempt_count = ?
            WHERE run_id = ?
            """,
            (
                status,
                row_count,
                json.dumps(metadata or {}, sort_keys=True),
                finished_at.replace(microsecond=0),
                duration_ms,
                error_message,
                json.dumps(sorted(dict.fromkeys(artifact_paths or [])), sort_keys=True),
                attempt_count,
                run_id,
            ),
        )
    finally:
        connection.close()


def list_ingestion_runs(
    db_path: Path,
    *,
    limit: int = 25,
    pipeline_name: str | None = None,
    status: str | None = None,
) -> list[dict[str, object]]:
    filters: list[str] = []
    params: list[object] = []
    if pipeline_name:
        filters.append("pipeline_name = ?")
        params.append(pipeline_name)
    if status:
        filters.append("status = ?")
        params.append(status)

    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.append(limit)
    return _fetch_dict_rows(
        db_path,
        f"""
        SELECT
          run_id,
          pipeline_name,
          status,
          row_count,
          metadata_json,
          error_message,
          artifact_paths_json,
          parent_run_id,
          attempt_count,
          CAST(started_at AS VARCHAR) AS started_at,
          CAST(finished_at AS VARCHAR) AS finished_at,
          duration_ms
        FROM ingestion_runs
        {where_sql}
        ORDER BY started_at DESC, run_id DESC
        LIMIT ?
        """,
        params,
    )


def get_latest_successful_runs(
    db_path: Path,
    pipeline_names: list[str],
) -> dict[str, dict[str, object]]:
    latest: dict[str, dict[str, object]] = {}
    for pipeline_name in pipeline_names:
        runs = list_ingestion_runs(
            db_path,
            limit=1,
            pipeline_name=pipeline_name,
            status="success",
        )
        if runs:
            latest[pipeline_name] = runs[0]
    return latest


def _row_to_dict(columns: list[str], row: tuple[object, ...]) -> dict[str, object]:
    record = dict(zip(columns, row))
    _parse_json_field(record, "explanation_payload", "explanation_payload")
    _parse_json_field(record, "metadata_json", "metadata")
    _parse_json_field(record, "artifact_paths_json", "artifact_paths")
    return record


def _parse_json_field(record: dict[str, object], source_key: str, target_key: str) -> None:
    payload = record.get(source_key)
    if not isinstance(payload, str):
        return
    try:
        record[target_key] = json.loads(payload)
    except json.JSONDecodeError:
        return
    if target_key != source_key:
        record.pop(source_key, None)


def _coerce_started_at(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    raise TypeError(f"Unsupported started_at value: {value!r}")
