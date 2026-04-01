from __future__ import annotations

import gzip
import json
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .models import RawFilingRecord, TickerReference

SEC_TICKER_REFERENCE_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_BASE_URL = "https://data.sec.gov/submissions"
DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5


class SecFetchError(RuntimeError):
    """Raised when an SEC request fails after retry handling."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def zero_pad_cik(cik: str | int) -> str:
    return str(cik).strip().zfill(10)


def build_submission_url(cik: str | int) -> str:
    return f"{SEC_SUBMISSIONS_BASE_URL}/CIK{zero_pad_cik(cik)}.json"


def fetch_json(
    url: str,
    user_agent: str,
    timeout_seconds: int = 30,
    *,
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    metrics: dict[str, object] | None = None,
) -> dict[str, Any]:
    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Accept": "application/json",
    }
    request = Request(url, headers=headers)
    context = _build_ssl_context()
    attempts = 0
    rate_limited = False
    last_error: str | None = None

    while attempts < max(retry_attempts, 1):
        attempts += 1
        try:
            with urlopen(request, timeout=timeout_seconds, context=context) as response:
                payload = response.read()
                if response.headers.get("Content-Encoding", "").lower() == "gzip":
                    payload = gzip.decompress(payload)
            parsed = json.loads(payload.decode("utf-8"))
            _populate_fetch_metrics(metrics, attempts=attempts, rate_limited=rate_limited)
            return parsed
        except HTTPError as exc:
            last_error = f"HTTP {exc.code}: {exc.reason}"
            rate_limited = rate_limited or exc.code == 429
            if not _should_retry_http_error(exc) or attempts >= retry_attempts:
                break
        except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            last_error = str(exc)
            if attempts >= retry_attempts:
                break

        time.sleep(_compute_backoff_delay(retry_backoff_seconds, attempts))

    _populate_fetch_metrics(
        metrics,
        attempts=attempts,
        rate_limited=rate_limited,
        last_error=last_error,
    )
    raise SecFetchError(last_error or f"Failed to fetch SEC payload from {url}")


def parse_company_tickers(
    payload: dict[str, Any],
    source_url: str = SEC_TICKER_REFERENCE_URL,
    retrieved_at: str | None = None,
) -> list[TickerReference]:
    collected_at = retrieved_at or utc_now_iso()
    if isinstance(payload, dict):
        records = payload.values()
    else:
        records = payload

    references = []
    for entry in records:
        ticker = str(entry["ticker"]).upper()
        references.append(
            TickerReference(
                ticker=ticker,
                cik=zero_pad_cik(entry["cik_str"]),
                company_name=str(entry["title"]).strip(),
                source_url=source_url,
                retrieved_at=collected_at,
            )
        )
    return sorted(references, key=lambda item: item.ticker)


def parse_recent_filings(
    payload: dict[str, Any],
    ticker: str,
    raw_path: Path,
    ingested_at: str | None = None,
) -> list[RawFilingRecord]:
    filings = payload.get("filings", {}).get("recent", {})
    accession_numbers = filings.get("accessionNumber", [])
    company_name = str(payload.get("name", ticker)).strip()
    cik = zero_pad_cik(payload.get("cik", ""))
    collected_at = ingested_at or utc_now_iso()

    records: list[RawFilingRecord] = []
    for index, accession_no in enumerate(accession_numbers):
        form_type = _safe_get(filings, "form", index) or "UNKNOWN"
        filing_date = _safe_get(filings, "filingDate", index)
        accepted_at = normalize_acceptance_datetime(_safe_get(filings, "acceptanceDateTime", index))
        items_json = normalize_items_json(_safe_get(filings, "items", index))
        primary_document = _safe_get(filings, "primaryDocument", index)
        primary_doc_description = _safe_get(filings, "primaryDocDescription", index)

        archive_accession = str(accession_no).replace("-", "")
        source_url = build_submission_url(cik)
        if primary_document:
            source_url = (
                f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{archive_accession}/{primary_document}"
            )

        records.append(
            RawFilingRecord(
                filing_id=f"{cik}:{accession_no}",
                ticker=ticker.upper(),
                cik=cik,
                company_name=company_name,
                accession_no=str(accession_no),
                form_type=str(form_type),
                filing_date=filing_date,
                accepted_at=accepted_at,
                items_json=items_json,
                primary_document=primary_document,
                primary_doc_description=primary_doc_description,
                source_url=source_url,
                raw_path=str(raw_path),
                ingested_at=collected_at,
            )
        )
    return records


def normalize_acceptance_datetime(value: str | None) -> str | None:
    if not value:
        return None

    text = value.strip()
    formats = (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y%m%d%H%M%S",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return text


def normalize_items_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        items = [_normalize_item_token(item) for item in value if str(item).strip()]
        return json.dumps(items) if items else None

    text = str(value).strip()
    if not text:
        return None

    separators = [",", ";", "|"]
    normalized = text
    for separator in separators:
        normalized = normalized.replace(separator, " ")
    items = [_normalize_item_token(token) for token in normalized.split() if token.strip()]
    unique_items = list(dict.fromkeys(items))
    return json.dumps(unique_items) if unique_items else None


def load_or_fetch_reference_map(paths, user_agent: str, refresh: bool = False) -> dict[str, TickerReference]:
    cache_path = paths.raw_dir / "sec" / "company_tickers.json"
    if refresh or not cache_path.exists():
        payload = fetch_json(SEC_TICKER_REFERENCE_URL, user_agent=user_agent)
        references = parse_company_tickers(payload)
        persist_reference_snapshot(paths, payload, references)
    else:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        retrieved_at = datetime.fromtimestamp(cache_path.stat().st_mtime, tz=timezone.utc).isoformat()
        references = parse_company_tickers(payload, retrieved_at=retrieved_at)
    return {reference.ticker: reference for reference in references}


def persist_reference_snapshot(paths, payload: dict[str, Any], references: list[TickerReference]) -> tuple[Path, Path]:
    raw_path = paths.raw_dir / "sec" / "company_tickers.json"
    bronze_path = paths.bronze_dir / "sec" / "ticker_reference.ndjson"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    raw_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    with bronze_path.open("w", encoding="utf-8") as handle:
        for reference in references:
            handle.write(json.dumps(reference.as_dict(), sort_keys=True) + "\n")
    return raw_path, bronze_path


def persist_submission_snapshot(paths, cik: str | int, payload: dict[str, Any]) -> Path:
    raw_path = paths.raw_dir / "sec" / "submissions" / f"CIK{zero_pad_cik(cik)}.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return raw_path


def persist_filing_snapshot(paths, filings: list[RawFilingRecord]) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bronze_path = paths.bronze_dir / "sec" / f"filings_{timestamp}.ndjson"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    with bronze_path.open("w", encoding="utf-8") as handle:
        for filing in filings:
            handle.write(json.dumps(filing.as_dict(), sort_keys=True) + "\n")
    return bronze_path


def _build_ssl_context() -> ssl.SSLContext:
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


def _safe_get(values_by_key: dict[str, list[Any]], key: str, index: int) -> Any:
    values = values_by_key.get(key, [])
    if index >= len(values):
        return None
    return values[index]


def _normalize_item_token(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return text
    return text.replace("Item", "").replace("item", "").strip()


def _populate_fetch_metrics(
    metrics: dict[str, object] | None,
    *,
    attempts: int,
    rate_limited: bool,
    last_error: str | None = None,
) -> None:
    if metrics is None:
        return
    metrics["attempt_count"] = attempts
    metrics["rate_limited"] = rate_limited
    if last_error:
        metrics["last_error"] = last_error


def _should_retry_http_error(error: HTTPError) -> bool:
    return error.code == 429 or 500 <= error.code < 600


def _compute_backoff_delay(base_delay: float, attempt: int) -> float:
    scaled_delay = max(base_delay, 0.0) * (2 ** max(attempt - 1, 0))
    return min(scaled_delay, 30.0)
