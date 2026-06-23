# ADR 003: Partition key strategy for raw-system-logs topic

## Decision

Partition the `raw-system-logs` topic by `service` name.

## Context

System logs need to be correlated per service. If logs from
`payments-service` land on different partitions they could be
processed out of order — making it impossible to reconstruct
the sequence of events that led to a failure.

## Why service and not something else

**Random / no key** — rejected.
Even distribution across partitions destroys ordering.
You cannot reconstruct "warning → warning → error" sequence
for a specific service if those events land on different partitions.

**log_level** — rejected.
All ERROR logs on one partition, all INFO on another. Creates
severe hot partition problem — INFO is 60% of all events.
Also meaningless for ordering — you need order within a service,
not within a log level.

**service name** — chosen.
All logs for `payments-service` always go to the same partition.
Downstream consumers can reconstruct the exact sequence of events
for any service. Anomaly detection can correctly identify
"3 warnings followed by an error" patterns.

## Trade-off

Only 4 services today. With 3 partitions, partition 0 may be
underutilised. As Meridian grows and more services are added
this will naturally balance out.

## Status

Accepted
