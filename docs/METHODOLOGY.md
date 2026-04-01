# Methodology

## Problem Framing

This system ranks unusual pre-disclosure market activity around official corporate events. It does not attempt to identify traders or prove illegal insider trading.

## Data Sources

- SEC filings such as `8-K` and `6-K`
- official issuer press releases and earnings releases from configured RSS or Atom feeds
- daily and minute-level market data
- optional local NLP models for richer sentiment and novelty scoring

## Event Construction

1. Ingest official source documents and preserve raw snapshots.
2. Normalize timestamps, issuer identity, and source metadata.
3. Build canonical events with one public timestamp per event.
4. Enrich events with sentiment, event type, novelty, and impact features.

## Feature Engineering

- daily abnormal return and abnormal volume features
- minute-level drift, realized volatility, and pre-release activity features
- source quality and timestamp confidence features
- event novelty, sentiment magnitude, and event-impact priors

## Scoring Stack

The ranking engine uses three layers:

- deterministic rules as a baseline and fallback
- `IsolationForest` over engineered daily and minute features
- `LightGBM` ranking on top of the anomaly signal and NLP features when a trained bundle is available

## Public Release Policy

- delayed public-safe serving by default
- no trader identification
- no claim that a high score is evidence of misconduct
- visible methodology, limitations, and evaluation pages

## Evaluation

The evaluation workflow is based on reviewed suspicious versus control events. Historical backtests report `Precision@K`, top-decile lift, and ablations across rules, NLP-only, market-only, anomaly-only, and hybrid engines.
