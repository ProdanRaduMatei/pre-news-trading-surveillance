# Backtest Report

- Generated at: `2026-04-02T05:42:48+00:00`
- Reviewed benchmark events: `24`
- Suspicious labels: `12`
- Control labels: `12`
- Base rate: `0.5000`

## Aggregate Metrics

| Engine | Precision@5 | Precision@10 | Precision@25 | Top-Decile Lift |
| --- | ---: | ---: | ---: | ---: |
| rules | 0.4167 | 0.4167 | 0.4167 | 2.5000 |
| nlp_only | 0.4167 | 0.4167 | 0.4167 | 2.5000 |
| market_only | 0.4167 | 0.4167 | 0.4167 | 0.5000 |
| anomaly_only | 0.4167 | 0.4167 | 0.4167 | 0.5000 |
| hybrid | 0.4167 | 0.4167 | 0.4167 | 0.5000 |

## Ablations

| Hybrid vs | Delta Precision@5 | Delta Precision@10 | Delta Precision@25 | Delta Top-Decile Lift |
| --- | ---: | ---: | ---: | ---: |
| rules | 0.0000 | 0.0000 | 0.0000 | -2.0000 |
| nlp_only | 0.0000 | 0.0000 | 0.0000 | -2.0000 |
| market_only | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
| anomaly_only | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
