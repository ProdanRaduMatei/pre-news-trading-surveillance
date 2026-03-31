from __future__ import annotations

import json
import re
from typing import Iterable

from .base import EventTypeResult

SEC_ITEM_EVENT_TYPE = {
    "1.01": "major_business_event",
    "1.02": "major_business_event",
    "1.03": "litigation_regulatory",
    "2.01": "mna",
    "2.02": "earnings",
    "2.03": "financing",
    "2.04": "financing",
    "2.05": "major_business_event",
    "2.06": "litigation_regulatory",
    "3.01": "litigation_regulatory",
    "3.02": "financing",
    "4.02": "litigation_regulatory",
    "5.02": "executive_change",
    "5.03": "major_business_event",
    "7.01": "major_business_event",
    "8.01": "other",
    "9.01": "other",
}

EVENT_TYPE_KEYWORDS = (
    ("mna", ("acquisition", "merger", "combination", "purchase agreement", "business combination")),
    ("guidance", ("guidance", "outlook", "forecast", "expects", "raises outlook", "cuts outlook")),
    ("earnings", ("results of operations", "earnings", "financial condition", "quarterly results")),
    ("executive_change", ("departure of directors", "appointment", "resignation", "chief executive", "officer")),
    ("financing", ("equity securities", "debt", "financing", "credit agreement", "offering", "convertible notes")),
    ("litigation_regulatory", ("lawsuit", "litigation", "subpoena", "investigation", "delisting", "regulatory", "non-reliance")),
    ("major_business_event", ("material definitive agreement", "partnership", "approval", "contract", "launch", "agreement")),
)

GENERIC_ITEMS = {"7.01", "8.01", "9.01"}


def parse_sec_items(items_json: str | None) -> list[str]:
    if not items_json:
        return []

    try:
        value = json.loads(items_json)
    except json.JSONDecodeError:
        value = items_json

    if isinstance(value, list):
        items = [normalize_sec_item(item) for item in value]
    else:
        items = [normalize_sec_item(token) for token in str(value).split()]
    return [item for item in items if item]


def classify_event_type(
    form_type: str,
    primary_doc_description: str | None,
    primary_document: str | None,
    sec_items: Iterable[str],
) -> EventTypeResult:
    normalized_items = [normalize_sec_item(item) for item in sec_items if normalize_sec_item(item)]
    text = build_event_text(form_type, primary_doc_description, primary_document).lower()
    reasons: list[str] = []

    for event_type, keywords in EVENT_TYPE_KEYWORDS:
        if any(keyword in text for keyword in keywords):
            reasons.append(f"keyword:{event_type}")
            if not _only_generic_items(normalized_items):
                item_type = _best_item_type(normalized_items)
                if item_type and item_type == event_type:
                    reasons.append(f"sec_item:{item_type}")
                    return EventTypeResult(event_type, 0.95, "sec_item_keyword_rules", reasons)
            return EventTypeResult(event_type, 0.8, "keyword_rules", reasons)

    item_type = _best_item_type(normalized_items)
    if item_type:
        reasons.append(f"sec_item:{item_type}")
        confidence = 0.9 if item_type != "other" else 0.55
        return EventTypeResult(item_type, confidence, "sec_item_rules", reasons)

    if form_type.upper() == "8-K":
        reasons.append("form_type:8-K")
        return EventTypeResult("major_business_event", 0.5, "form_type_fallback", reasons)

    reasons.append("fallback:other")
    return EventTypeResult("other", 0.35, "fallback_rules", reasons)


def build_event_text(form_type: str, primary_doc_description: str | None, primary_document: str | None) -> str:
    return " ".join(
        part.strip()
        for part in [form_type or "", primary_doc_description or "", primary_document or ""]
        if part and part.strip()
    )


def normalize_sec_item(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    text = text.replace("Item", "").replace("item", "").strip()
    match = re.search(r"(\d+\.\d+)", text)
    return match.group(1) if match else text


def _only_generic_items(items: list[str]) -> bool:
    return bool(items) and all(item in GENERIC_ITEMS for item in items)


def _best_item_type(items: list[str]) -> str | None:
    prioritized = ["2.01", "2.02", "2.03", "3.02", "5.02", "3.01", "4.02", "1.03", "2.06", "1.01", "1.02", "7.01", "8.01"]
    item_set = set(items)
    for item in prioritized:
        if item in item_set:
            return SEC_ITEM_EVENT_TYPE.get(item)
    for item in items:
        event_type = SEC_ITEM_EVENT_TYPE.get(item)
        if event_type:
            return event_type
    return None
