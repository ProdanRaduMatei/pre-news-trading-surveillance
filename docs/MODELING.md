# Modeling

## Modeling Objective

The product ranks event-level suspiciousness for unusual pre-disclosure activity. It does not estimate a legal probability of insider trading.

## Feature Families

### Event and NLP Features

- event type
- source quality
- timestamp confidence
- `FinBERT` sentiment
- novelty from sentence embeddings

### Market Features

- cumulative abnormal return
- sector-relative and market-relative return
- abnormal volume
- realized volatility shock
- gap percentage
- final-hour drift concentration

### Future Options Features

- abnormal call and put volume
- skew change
- implied volatility jump
- near-expiry concentration

## Layered Scoring

### Layer 1: Rule Score

Transparent heuristics that identify obvious directional drift plus volume anomalies before a high-confidence event.

### Layer 2: Anomaly Score

`IsolationForest` over market feature vectors to identify rare multivariate patterns.

### Layer 3: Ranker

`LightGBM` combines:

- event features
- NLP features
- market features
- anomaly score
- metadata

## Label Strategy

Use weak supervision:

- positives from historical SEC enforcement material
- matched controls by sector, size, and event type
- unknown bucket for everything else

## Evaluation Requirements

- time-based train, validation, and test splits only
- `Precision@K`
- top-decile lift
- `NDCG`
- ablations against rule-only, market-only, and NLP-only variants

## Explainability

Every score must be backed by deterministic feature outputs stored at scoring time. LLM-generated explanations, if used, should only rewrite existing evidence rather than invent reasoning.
