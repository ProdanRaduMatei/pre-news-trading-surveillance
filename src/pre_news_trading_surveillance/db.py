from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from .ingest.models import RawFilingRecord, TickerReference


def _require_duckdb():
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "DuckDB is required for database commands. Install dependencies with `pip install -e .`."
        ) from exc
    return duckdb


def init_database(db_path: Path, schema_path: Path) -> Path:
    duckdb = _require_duckdb()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = schema_path.read_text(encoding="utf-8")
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute(schema_sql)
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
              primary_document,
              primary_doc_description,
              source_url,
              raw_path,
              ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


def record_ingestion_run(
    db_path: Path,
    pipeline_name: str,
    status: str,
    row_count: int,
    metadata: dict[str, object] | None = None,
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
              started_at
            )
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                run_id,
                pipeline_name,
                status,
                row_count,
                json.dumps(metadata or {}, sort_keys=True),
            ),
        )
    finally:
        connection.close()
    return run_id
