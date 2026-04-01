# Evaluation

## Goal

The evaluation layer measures whether the ranking stack surfaces reviewed suspicious events earlier than simpler baselines. It is designed for research credibility, not legal adjudication.

## Benchmark Workflow

1. Export a manual review queue with `pnts export-benchmark-candidates`.
2. Review candidates as `suspicious`, `control`, or `unknown`.
3. Import reviewed labels with `pnts import-benchmark-labels`.
4. Run `pnts run-backtest` to generate JSON and Markdown reports.

Starter files:

- review queue output: `data/benchmarks/review_candidates.csv`
- reviewed-label template: `data/benchmarks/reviewed_benchmark.template.csv`

## Metrics

The backtest report currently includes:

- `Precision@K`
- top-decile lift
- per-fold chronological results
- ablations for:
  - `rules`
  - `nlp_only`
  - `market_only`
  - `anomaly_only`
  - `hybrid`

## Split Strategy

- chronological folds only
- expanding training window
- contiguous time-based test slices

## Output

Reports are written to `reports/evaluation/` as:

- `backtest_report_<timestamp>.json`
- `backtest_report_<timestamp>.md`

The evaluation run also records artifacts and summary metadata in `ingestion_runs`.
