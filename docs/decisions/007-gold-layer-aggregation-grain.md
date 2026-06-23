# ADR 007: Gold layer aggregation grain — merchant per hour

## Decision

Aggregate Silver payment data at the grain of (merchant_id, hour).

## Context

Gold layer aggregates must be defined at a specific "grain" —
the level of detail each row represents. This choice determines
what questions Gold can answer directly and what it cannot.

## Alternatives considered

**Per merchant per day** — rejected.
Too coarse for anomaly detection. A 2-hour failure spike would
be diluted across 24 hours of normal data, hiding the signal.
Meridian's promise is catching problems quickly, not waiting
for tomorrow's daily report.

**Per merchant per minute** — rejected.
Too granular. Most merchants don't have enough transaction volume
per minute for failure rate percentages to be statistically
meaningful. A single failed transaction would show as 100%
failure rate on a low-volume minute, creating false alarms.

**Per merchant per hour** — chosen.
Fine enough to catch real-time problems within the hour they occur.
Coarse enough that failure rate percentages are statistically
meaningful even for moderate-volume merchants. Matches how most
payment platforms define "normal" failure rate baselines.

## What this enables

- Compare current hour vs same hour last week (seasonality aware)
- Detect failure rate spikes within an hour of occurring
- Aggregate further into daily/weekly views without losing the
  ability to drill back down to hourly detail

## Trade-off

An anomaly that resolves within 10 minutes might not show as
a dramatic spike if it's diluted across the rest of the hour.
Future Flink layer solves this — it operates on sub-minute
windows for true real-time detection. Gold's hourly grain is for
historical analysis and trend reporting, not real-time alerting.

## Status

Accepted
