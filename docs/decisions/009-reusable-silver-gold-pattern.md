# ADR 009: Reusable Silver/Gold pattern across event streams

## Decision

Apply the same four-step Silver transformation (null check, business
rule validation, deduplication, timestamp parsing) and the same Gold
aggregation shape (group by entity + hour, compute rate/health metrics)
across all three event streams — payments, user events, and system logs.

## Context

After building Silver and Gold independently for payments, the same
work was needed for user_events and system_logs. Each event type has
a different schema and different business meaning, but the underlying
data quality problems are the same: missing fields, invalid values,
duplicate delivery from Kafka, and string timestamps that need parsing.

## What stayed identical across all three pipelines

**Silver layer steps, same order, every time:**

1. Drop rows with null critical fields
2. Filter rows against domain-specific validation rules
3. Deduplicate by event_id using a window function, keep latest by
   kafka_timestamp
4. Parse the string timestamp into a proper timestamp type

**Gold layer shape, same pattern, every time:**

1. Bucket by hour using date_trunc
2. Group by the natural entity for that stream (merchant_id, device,
   service)
3. Aggregate counts conditioned on a categorical field (status,
   event_type, level) using count(when(...))
4. Compute a rate metric as a percentage of total

## What changed per stream

|                   | Payments                   | User events                    | System logs                     |
| ----------------- | -------------------------- | ------------------------------ | ------------------------------- |
| Entity            | merchant_id                | device                         | service                         |
| Categorical field | status                     | event_type                     | level                           |
| Rate metric       | failure_rate_pct           | (funnel, no single rate)       | error_rate_pct                  |
| Extra validation  | amount > 0, known currency | known event_type, known device | latency >= 0, percentages 0-100 |

## Why this matters

Recognizing the repeated shape early means new event streams can be
added to Meridian in roughly 15-20 minutes instead of an hour, by
copying the pattern and changing only the validation rules and the
grouping entity. This is the same reasoning that would apply if
Meridian were extended to a new domain entirely (see FUTURE.md —
Shopify connector idea) — the medallion pattern doesn't change,
only the schema and the business-specific validation rules do.

## Trade-off

Three near-identical files exist instead of one parameterized
pipeline. A more DRY approach would extract a shared base function
taking schema, validation rules, and grouping entity as parameters.
Deliberately not done yet — at three pipelines the duplication is
still easier to read and reason about than an abstraction layer.
Revisit if a fourth or fifth event stream is added.

## Status

Accepted
