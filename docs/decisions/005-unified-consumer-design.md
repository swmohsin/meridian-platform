# ADR 005: Unified consumer reading from multiple topics

## Decision

Build a single consumer that subscribes to all three topics
simultaneously rather than three separate single-topic consumers.

## Context

Meridian's core promise is correlating events across payments,
user activity, and system health in real time. To fulfill that
promise something must read all three streams together.

## Alternatives considered

**Three separate consumers, one per topic** — rejected.
Each consumer sees only its own stream. Correlating events
across streams requires joining three independent processes —
complex, error prone, and defeats the purpose of a unified
platform. If payments-service throws an error at the same
moment payment failures spike, three separate consumers
cannot see that connection. One unified consumer can.

**One consumer, poll topics sequentially** — rejected.
Polling topics one at a time introduces latency. While
consumer A is polling payments, user events and system logs
are waiting. In a real anomaly scenario where milliseconds
matter, sequential polling is too slow.

**Single consumer subscribed to all topics** — chosen.
Kafka's consumer API supports subscribing to multiple topics
in one call. The broker handles partition assignment across
all topics automatically. One consumer sees everything as it
arrives, in real time, with no coordination overhead.
This is the pattern used by stream processing frameworks
like Flink and Spark Structured Streaming internally.

## Trade-off

A single consumer is a single point of failure — if it crashes
all three streams stop being read. Acceptable for the demo layer.
In production the Spark and Flink jobs that replace this consumer
run on distributed clusters with automatic failover.

## What this unlocks

A single process that sees:

- A payment failure on merchant_002
- A simultaneous ERROR on payments-service
- A drop in user checkout events

...and can correlate all three as one incident rather than
three unrelated events. This is the foundation of Meridian's
AI anomaly detection layer.

## Status

Accepted
