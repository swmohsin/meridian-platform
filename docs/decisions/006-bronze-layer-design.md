# ADR 006: Bronze layer — raw persistence without transformation

## Decision

The Bronze layer writes Kafka events to Delta Lake exactly as received —
no cleaning, no transformation, no business logic applied.

## Context

When building a data lakehouse you must decide whether to clean
data on the way in or store it raw first. This decision is permanent —
you cannot go back and see the original if you transform on ingest.

## Alternatives considered

**Transform on ingest** — rejected.
Clean and validate events as they arrive from Kafka.
Simpler pipeline but catastrophic if your cleaning logic has a bug.
If you accidentally drop a field or misparse a value, that data
is gone forever. No way to recover the original.

**Store raw first, transform later** — chosen.
Bronze stores exactly what Kafka delivered — including malformed
events, duplicates, and schema violations. Silver then cleans
from Bronze. If Silver logic has a bug you reprocess from Bronze.
Original data is always recoverable.

This is the medallion architecture pattern used at Databricks,
Uber, Airbnb, and Netflix. It is the industry standard for
production data lakehouses.

## What Bronze stores

- All fields from the original event
- Kafka partition and offset (for traceability)
- Kafka timestamp (when broker received the message)
- No deduplication
- No validation
- No enrichment

## Trade-off

Storing raw data including duplicates and bad records uses more
disk space than storing only clean data. Acceptable — storage
is cheap, lost data is not recoverable.

## Status

Accepted
