# ADR 001: Partition key strategy for raw-payments topic

## Decision

Partition the `raw-payments` topic by `merchant_id`.

## Context

Kafka assigns messages to partitions based on a hash of the message key.
All messages with the same key always go to the same partition.
This guarantees ordering for all events belonging to the same key.

## Why merchant_id and not something else

**user_id** — rejected. A user makes occasional purchases across many
merchants. No business logic requires strict ordering across a user's
entire payment history.

**event_id** — rejected. Random keys distribute evenly but destroy
ordering entirely. Two events for the same merchant could land on
different partitions and be processed out of order.

**merchant_id** — chosen. All payment events for a given merchant
must be processed in order. A payment reversal that arrives before
its original charge would produce incorrect merchant balance calculations.

## Trade-off

Hot partitions are possible if one merchant generates significantly
more volume than others. A large merchant could overwhelm one partition
while others sit idle.

Mitigation: monitor partition lag per merchant. If any single merchant
exceeds 10x average volume, consider sub-partitioning by merchant_id +
date suffix.

## Status

Accepted
