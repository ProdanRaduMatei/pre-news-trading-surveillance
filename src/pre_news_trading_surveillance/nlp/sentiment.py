from __future__ import annotations

from .base import SentimentResult
from .sec_taxonomy import build_event_text, parse_sec_items

POSITIVE_KEYWORDS = {
    "acquisition": 0.45,
    "approval": 0.4,
    "agreement": 0.2,
    "partnership": 0.35,
    "launch": 0.25,
    "dividend": 0.25,
    "results": 0.2,
    "raises outlook": 0.6,
}
NEGATIVE_KEYWORDS = {
    "litigation": -0.45,
    "investigation": -0.5,
    "delisting": -0.8,
    "resignation": -0.25,
    "impairment": -0.55,
    "bankruptcy": -0.9,
    "restatement": -0.7,
    "guidance cut": -0.65,
    "non-reliance": -0.85,
}
POSITIVE_ITEMS = {"2.01", "7.01"}
NEGATIVE_ITEMS = {"1.03", "2.04", "2.06", "3.01", "4.02"}
NEUTRAL_ITEMS = {"2.02", "5.02", "8.01", "9.01"}


class HeuristicSentimentBackend:
    name = "heuristic"

    def analyze(
        self,
        form_type: str,
        primary_doc_description: str | None,
        primary_document: str | None,
        event_type: str,
        sec_items_json: str | None,
    ) -> SentimentResult:
        text = build_event_text(form_type, primary_doc_description, primary_document).lower()
        sec_items = parse_sec_items(sec_items_json)
        score = 0.0

        for phrase, weight in POSITIVE_KEYWORDS.items():
            if phrase in text:
                score += weight
        for phrase, weight in NEGATIVE_KEYWORDS.items():
            if phrase in text:
                score += weight

        for item in sec_items:
            if item in POSITIVE_ITEMS:
                score += 0.2
            elif item in NEGATIVE_ITEMS:
                score -= 0.35
            elif item in NEUTRAL_ITEMS:
                score += 0.0

        if score == 0.0 and event_type in {"mna", "guidance"}:
            score = 0.15

        label = "neutral"
        if score >= 0.15:
            label = "positive"
        elif score <= -0.15:
            label = "negative"

        confidence = min(0.9, 0.5 + abs(score) / 2.0)
        return SentimentResult(
            label=label,
            score=round(max(-1.0, min(1.0, score)), 4),
            confidence=round(confidence, 4),
            backend=self.name,
            raw_label=label,
        )


class TransformersFinBertBackend:
    name = "finbert"

    def __init__(self, model_name_or_path: str) -> None:
        try:
            from transformers import pipeline
        except ImportError as exc:
            raise RuntimeError(
                "transformers is required for the FinBERT backend. "
                "Install it with `pip install -e .[nlp]` and point to a local model path if you want fully on-device inference."
            ) from exc

        local_files_only = "/" in model_name_or_path or model_name_or_path.startswith(".")
        self._pipeline = pipeline(
            "text-classification",
            model=model_name_or_path,
            tokenizer=model_name_or_path,
            truncation=True,
            local_files_only=local_files_only,
        )

    def analyze(
        self,
        form_type: str,
        primary_doc_description: str | None,
        primary_document: str | None,
        event_type: str,
        sec_items_json: str | None,
    ) -> SentimentResult:
        text = build_event_text(form_type, primary_doc_description, primary_document)
        result = self._pipeline(text[:2000])[0]
        raw_label = str(result.get("label", "neutral")).lower()
        score = float(result.get("score", 0.0))

        if "positive" in raw_label:
            label = "positive"
            signed_score = score
        elif "negative" in raw_label:
            label = "negative"
            signed_score = -score
        else:
            label = "neutral"
            signed_score = 0.0

        return SentimentResult(
            label=label,
            score=round(signed_score, 4),
            confidence=round(score, 4),
            backend=self.name,
            raw_label=raw_label,
        )


def build_sentiment_backend(name: str, model_name_or_path: str | None = None):
    normalized = name.lower()
    if normalized in {"heuristic", "rules"}:
        return HeuristicSentimentBackend()
    if normalized in {"finbert", "transformers"}:
        model = model_name_or_path or "ProsusAI/finbert"
        return TransformersFinBertBackend(model)
    raise ValueError(f"Unknown sentiment backend: {name}")
