# Risk and Limitations

## Product Boundary

This project is a research and surveillance-ranking system. It is not an accusation engine and must never present outputs as proof of wrongdoing.

## Core Limitations

- public data generally does not reveal trader identity in real time
- timestamp quality can be imperfect outside official sources
- weak labels are incomplete and delayed
- market anticipation, rumor leakage, and sector sympathy moves create false positives

## Public Release Risks

### Misinterpretation Risk

Users may read a high score as proof of illegal activity. The UI and API must actively prevent that framing.

### Reputational Risk

Naming individuals or making unsupported allegations would be high risk. Public product copy must avoid that entirely outside well-sourced historical case studies.

### Model Risk

The model may over-rank volatile names or recurring event types unless carefully normalized and evaluated.

## Mitigations

- delayed public refresh
- conservative score bands
- visible methodology and disclaimer pages
- analyst-review framing
- audit trail of how each score was produced
