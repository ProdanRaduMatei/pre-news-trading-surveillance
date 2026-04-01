from __future__ import annotations

import hashlib
import html
import json
import re
import ssl
import time
import tomllib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .market import DEFAULT_USER_AGENT
from .models import IssuerFeedConfig, RawIssuerReleaseRecord

DEFAULT_RETRY_ATTEMPTS = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5


class IssuerReleaseIngestError(RuntimeError):
    """Raised when an issuer release feed cannot be fetched or parsed."""


def load_feed_configs(config_path: Path, tickers: Iterable[str] | None = None) -> list[IssuerFeedConfig]:
    payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
    requested = {ticker.upper() for ticker in tickers or []}
    feeds = []
    for entry in payload.get("feeds", []):
        ticker = str(entry["ticker"]).upper()
        if requested and ticker not in requested:
            continue
        feeds.append(
            IssuerFeedConfig(
                ticker=ticker,
                issuer_name=str(entry["issuer_name"]).strip(),
                feed_url=str(entry["feed_url"]).strip(),
                source_name=str(entry.get("source_name", f"{ticker} official feed")).strip(),
                official_homepage=_none_if_blank(entry.get("official_homepage")),
            )
        )
    if requested:
        missing = sorted(requested - {feed.ticker for feed in feeds})
        if missing:
            raise IssuerReleaseIngestError(
                f"Missing issuer feed config for ticker(s): {', '.join(missing)}"
            )
    return feeds


def fetch_feed_xml(
    url: str,
    *,
    user_agent: str = DEFAULT_USER_AGENT,
    timeout_seconds: int = 30,
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    metrics: dict[str, object] | None = None,
) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml",
        },
    )
    context = _build_ssl_context()
    attempts = 0
    rate_limited = False
    last_error: str | None = None

    while attempts < max(retry_attempts, 1):
        attempts += 1
        try:
            with urlopen(request, timeout=timeout_seconds, context=context) as response:
                text = response.read().decode("utf-8", errors="replace")
            _populate_metrics(metrics, attempts=attempts, rate_limited=rate_limited)
            return text
        except HTTPError as exc:
            last_error = f"HTTP {exc.code}: {exc.reason}"
            rate_limited = rate_limited or exc.code == 429
            if not _should_retry_http_error(exc) or attempts >= retry_attempts:
                break
        except (URLError, TimeoutError, OSError) as exc:
            last_error = str(exc)
            if attempts >= retry_attempts:
                break
        time.sleep(_compute_backoff_delay(retry_backoff_seconds, attempts))

    _populate_metrics(metrics, attempts=attempts, rate_limited=rate_limited, last_error=last_error)
    raise IssuerReleaseIngestError(last_error or f"Failed to fetch issuer release feed: {url}")


def parse_feed_releases(
    xml_text: str,
    *,
    feed: IssuerFeedConfig,
    raw_path: Path,
    per_feed_limit: int = 25,
    ingested_at: str | None = None,
) -> list[RawIssuerReleaseRecord]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise IssuerReleaseIngestError(f"Unable to parse feed XML for {feed.ticker}: {exc}") from exc

    entries = _parse_atom_entries(root, feed) if _local_name(root.tag) == "feed" else _parse_rss_entries(root, feed)
    collected_at = ingested_at or _utc_now_iso()
    releases: list[RawIssuerReleaseRecord] = []
    for entry in entries[: max(per_feed_limit, 0)]:
        published_at = _parse_feed_datetime(entry.get("published_at"))
        identity_source = entry.get("entry_guid") or entry.get("source_url") or entry["title"]
        release_hash = hashlib.sha1(f"{feed.ticker}|{identity_source}".encode("utf-8")).hexdigest()[:16]
        releases.append(
            RawIssuerReleaseRecord(
                release_id=f"issuer-release:{feed.ticker}:{release_hash}",
                ticker=feed.ticker,
                issuer_name=feed.issuer_name,
                source_name=feed.source_name,
                feed_url=feed.feed_url,
                entry_guid=entry.get("entry_guid"),
                title=entry["title"],
                summary_text=entry.get("summary_text"),
                source_url=entry.get("source_url") or feed.official_homepage or feed.feed_url,
                published_at=published_at,
                raw_path=str(raw_path),
                ingested_at=collected_at,
            )
        )

    releases.sort(key=lambda release: release.published_at or release.ingested_at, reverse=False)
    return releases


def persist_feed_snapshot(paths, *, feed: IssuerFeedConfig, xml_text: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"{feed.ticker}_{_slugify(feed.source_name)}_{timestamp}.xml"
    raw_path = paths.raw_dir / "issuer_releases" / filename
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(xml_text, encoding="utf-8")
    return raw_path


def persist_release_snapshot(paths, releases: list[RawIssuerReleaseRecord]) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bronze_path = paths.bronze_dir / "issuer_releases" / f"issuer_releases_{timestamp}.ndjson"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    with bronze_path.open("w", encoding="utf-8") as handle:
        for release in releases:
            handle.write(json.dumps(release.as_dict(), sort_keys=True) + "\n")
    return bronze_path


def _parse_rss_entries(root: ET.Element, feed: IssuerFeedConfig) -> list[dict[str, str | None]]:
    channel = next((child for child in root if _local_name(child.tag) == "channel"), root)
    entries = [child for child in channel if _local_name(child.tag) == "item"]
    parsed = []
    for entry in entries:
        title = _clean_text(_find_child_text(entry, "title")) or f"{feed.issuer_name} release"
        parsed.append(
            {
                "title": title,
                "summary_text": _extract_summary(entry),
                "source_url": _find_child_text(entry, "link"),
                "published_at": _find_child_text(entry, "pubDate") or _find_child_text(entry, "date"),
                "entry_guid": _find_child_text(entry, "guid"),
            }
        )
    return parsed


def _parse_atom_entries(root: ET.Element, feed: IssuerFeedConfig) -> list[dict[str, str | None]]:
    entries = [child for child in root if _local_name(child.tag) == "entry"]
    parsed = []
    for entry in entries:
        title = _clean_text(_find_child_text(entry, "title")) or f"{feed.issuer_name} release"
        parsed.append(
            {
                "title": title,
                "summary_text": _extract_summary(entry),
                "source_url": _extract_atom_link(entry),
                "published_at": _find_child_text(entry, "published") or _find_child_text(entry, "updated"),
                "entry_guid": _find_child_text(entry, "id"),
            }
        )
    return parsed


def _extract_summary(entry: ET.Element) -> str | None:
    for tag in ("description", "summary", "content", "encoded"):
        text = _find_child_text(entry, tag)
        cleaned = _clean_text(text)
        if cleaned:
            return cleaned
    return None


def _extract_atom_link(entry: ET.Element) -> str | None:
    for child in entry:
        if _local_name(child.tag) != "link":
            continue
        rel = (child.attrib.get("rel") or "alternate").strip().lower()
        href = child.attrib.get("href")
        if rel == "alternate" and href:
            return href.strip()
    for child in entry:
        if _local_name(child.tag) == "link" and child.attrib.get("href"):
            return child.attrib["href"].strip()
    return None


def _find_child_text(element: ET.Element, tag_name: str) -> str | None:
    for child in element.iter():
        if child is element:
            continue
        if _local_name(child.tag) == tag_name and child.text:
            return child.text.strip()
    return None


def _local_name(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _clean_text(value: str | None) -> str | None:
    if not value:
        return None
    text = html.unescape(value)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _parse_feed_datetime(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip()
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError):
        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _populate_metrics(
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


def _compute_backoff_delay(base_delay: float, attempt: int) -> float:
    scaled_delay = max(base_delay, 0.0) * (2 ** max(attempt - 1, 0))
    return min(scaled_delay, 30.0)


def _should_retry_http_error(error: HTTPError) -> bool:
    return error.code == 429 or 500 <= error.code < 600


def _build_ssl_context() -> ssl.SSLContext:
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


def _slugify(value: str) -> str:
    return "".join(character.lower() if character.isalnum() else "_" for character in value).strip("_")


def _none_if_blank(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
