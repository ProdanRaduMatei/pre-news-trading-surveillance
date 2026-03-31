from __future__ import annotations

import math
import re
from collections.abc import Iterable

from .base import NoveltyResult

STOPWORDS = {
    "and",
    "the",
    "for",
    "with",
    "from",
    "this",
    "that",
    "inc",
    "corp",
    "company",
    "form",
    "report",
    "current",
}


class LexicalNoveltyBackend:
    name = "lexical"

    def score(self, text: str, history_texts: Iterable[str]) -> NoveltyResult:
        target_tokens = _tokenize(text)
        comparisons = []
        for prior_text in history_texts:
            prior_tokens = _tokenize(prior_text)
            comparisons.append(_jaccard_similarity(target_tokens, prior_tokens))

        max_similarity = max(comparisons) if comparisons else 0.0
        novelty = 1.0 - max_similarity
        if comparisons and max_similarity > 0.85:
            novelty = max(0.05, novelty)
        novelty = max(0.0, min(1.0, novelty))
        confidence = 0.55 if comparisons else 0.7
        return NoveltyResult(
            score=round(novelty, 4),
            backend=self.name,
            confidence=confidence,
            max_similarity=round(max_similarity, 4),
            compared_items=len(comparisons),
        )


class SentenceTransformerNoveltyBackend:
    name = "sentence-transformers"

    def __init__(self, model_name_or_path: str) -> None:
        try:
            from sentence_transformers import SentenceTransformer
            from sentence_transformers.util import cos_sim
        except ImportError as exc:
            raise RuntimeError(
                "sentence-transformers is required for the sentence-transformers novelty backend. "
                "Install it with `pip install -e .[nlp]` and provide a local model path if you want fully on-device use."
            ) from exc

        self._cos_sim = cos_sim
        self._model = SentenceTransformer(model_name_or_path)

    def score(self, text: str, history_texts: Iterable[str]) -> NoveltyResult:
        history = list(history_texts)
        if not history:
            return NoveltyResult(1.0, self.name, 0.85, 0.0, 0)

        embeddings = self._model.encode([text, *history], convert_to_tensor=True)
        query_embedding = embeddings[0]
        history_embeddings = embeddings[1:]
        similarities = self._cos_sim(query_embedding, history_embeddings).flatten().tolist()
        max_similarity = max(float(value) for value in similarities) if similarities else 0.0
        novelty = max(0.0, min(1.0, 1.0 - max_similarity))
        return NoveltyResult(
            score=round(novelty, 4),
            backend=self.name,
            confidence=0.85,
            max_similarity=round(max_similarity, 4),
            compared_items=len(history),
        )


def build_novelty_backend(name: str, model_name_or_path: str | None = None):
    normalized = name.lower()
    if normalized in {"lexical", "heuristic"}:
        return LexicalNoveltyBackend()
    if normalized in {"sentence-transformers", "sentence_transformers", "embeddings"}:
        if not model_name_or_path:
            raise RuntimeError(
                "The sentence-transformers novelty backend requires `--novelty-model` with a local or accessible model."
            )
        return SentenceTransformerNoveltyBackend(model_name_or_path)
    raise ValueError(f"Unknown novelty backend: {name}")


def _tokenize(text: str) -> set[str]:
    tokens = {
        token
        for token in re.findall(r"[a-z0-9]{3,}", text.lower())
        if token not in STOPWORDS
    }
    return tokens


def _jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)
