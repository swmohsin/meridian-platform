# ADR 004: Partition key strategy for raw-user-events topic

## Decision

Partition the `raw-user-events` topic by `user_id`.

## Context

User activity events must be ordered per user to correctly
reconstruct their journey through the platform. A purchase
event processed before the corresponding add-to-cart event
would produce incorrect funnel analysis and false anomaly signals.

## Why user_id and not something else

**session_id** — rejected.
A single user can have multiple sessions across devices.
Partitioning by session_id would split one user's journey
across multiple partitions making cross-session analysis
impossible. Also sessions are short-lived — poor long term
distribution key.

**event_type** — rejected.
All page_view events on one partition, all purchase events
on another. Destroys per-user ordering entirely. Creates
severe hot partition problem since page_view is 33% of
all events. Useless for reconstructing user journeys.

**page** — rejected.
All events for /checkout on one partition. No relationship
to user identity. Cannot reconstruct any meaningful sequence.

**user_id** — chosen.
All events for a given user always land on the same partition.
Downstream consumers can reconstruct the exact sequence:
page_view → add_to_cart → checkout → purchase.
This is the sequence Meridian's AI layer needs to detect
abandoned carts, unusual purchase patterns, and account
takeover attempts.

## Trade-off

With 500 simulated users and 3 partitions, distribution
should be reasonably even. In production with millions of
users, distribution will be excellent. Power users generating
significantly more events than average could cause mild
hot partitions — acceptable given the ordering benefits.

## Status

Accepted
