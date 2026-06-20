# ADR 008: Statistical baseline approach for anomaly detection

## Decision

Use per-merchant mean and standard deviation of failure rate,
with a 2-sigma threshold, as the first layer of anomaly detection —
before any machine learning is introduced.

## Context

Meridian's core promise is detecting when something is wrong.
Before building AI agents to investigate anomalies, Meridian needs
a reliable, explainable way to define what "wrong" means in the
first place. This decision shapes every layer built on top of it.

## Alternatives considered

**Fixed threshold, same for all merchants** — rejected.
E.g. "alert if failure rate exceeds 25%". Fails because merchants
have genuinely different baseline failure rates depending on
business type, payment methods accepted, and customer base.
A merchant with 20% normal failure rate would get constant false
alarms. A merchant with 5% normal failure rate could have a real
problem at 15% and never get flagged.

**Train a machine learning model immediately** — rejected.
Requires labeled training data Meridian does not have. Adds
latency, training infrastructure, and a black box that is hard
to explain to stakeholders. Premature — statistical baselines
solve the majority of real anomaly cases without any of this cost.

**Per-merchant statistical baseline with 2-sigma threshold** — chosen.
Each merchant's own historical mean and standard deviation define
what "normal" looks like for them specifically. An event is flagged
only when it deviates significantly from that merchant's own
established pattern. Statistically grounded, fully explainable,
zero training data required, and computed directly from Gold
layer data already being produced.

## Why 2 standard deviations

In a normal distribution approximately 95% of values fall within
2 standard deviations of the mean. Flagging anything beyond that
catches genuinely unusual behavior while keeping false positive
rate low. This is a deliberately conservative starting threshold —
intended to be tuned per merchant as more historical data accumulates.

## What this unlocks

- Statistically meaningful anomaly flags with zero ML infrastructure
- A clear, explainable answer to "why was this flagged" —
  "31.25% failure rate vs this merchant's normal 16.6% ± 6.07%"
- A solid foundation for future AI agents can build on — the AI
  investigates WHY a statistically flagged anomaly occurred,
  rather than trying to detect the anomaly itself from scratch

## Trade-off

Requires sufficient historical data per merchant before the
baseline is statistically meaningful. New merchants with limited
history will have unreliable thresholds. Mitigation: require a
minimum number of hours of data before activating anomaly
detection for any given merchant.

Also assumes failure rate is roughly normally distributed.
Real-world data can be skewed — a future iteration could use
percentile-based thresholds (e.g. 95th percentile) instead of
standard deviation for greater robustness.

## Status

Accepted
