# ADR 002: Separate topic per event type

## Decision

Use three separate Kafka topics:

- `raw-payments`
- `raw-user-events`
- `raw-system-logs`

## Context

When designing a Kafka pipeline you can put all events into one topic
or separate them by type. This decision affects every downstream
consumer forever — it is hard to change later.

## Alternatives considered

**Single topic `raw-events`** — rejected.
All event types mixed together. Every consumer must filter out
the events it doesn't care about. Schema evolution becomes a nightmare —
changing the payment event schema risks breaking the system logs consumer.
Impossible to set different retention policies per event type.

**One topic per event type** — chosen.
Each topic has one clear responsibility. Consumers subscribe only
to what they need. Each topic can have its own retention period,
partition count, and replication factor tuned to its volume and
importance. Payment events might need 7 day retention.
System logs might only need 24 hours.

## Trade-off

More topics to manage. Monitoring and access control becomes
more granular. Worth it for the clean separation.

## Status

Accepted
