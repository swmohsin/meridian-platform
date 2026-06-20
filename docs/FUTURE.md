# Meridian — Future Directions

This file captures ideas considered for Meridian that are intentionally
**not** part of the current build. They are documented here so the
thinking isn't lost, without pulling focus from the core roadmap.

---

## Idea: Shopify connector

**Status:** Not started. Documented for later consideration.

### The idea

Meridian's core engine (Kafka → Bronze → Silver → Gold → statistical
baselines) is event-type agnostic. The pipeline doesn't care whether
events are payments, user activity, or e-commerce orders — it cares
about volume, ratios, and timing patterns relative to a baseline.

This means Meridian could be repackaged as a Shopify app: a thin
connector layer translating Shopify's event shapes into the same
pipeline already built for the core portfolio project.

### Why this is a separate effort, not a roadmap item

A Shopify app is a different product surface from a self-hosted
open source platform:

- Requires building inside Shopify's app framework (OAuth, App Bridge,
  Billing API)
- Requires Shopify App Store review and compliance
- Requires a UI living inside the Shopify admin dashboard, not just
  a CLI/API platform
- Distribution model is fundamentally different — App Store discovery
  vs GitHub stars and self-hosting

None of this serves the immediate goal: a portfolio project that
demonstrates distributed systems, data engineering, and AI engineering
depth.

### What would change if built

**New event types** (replacing the generic payments/users/logs demo data):

```
raw-orders       — order placed: order_id, line items, total, customer_id
raw-checkouts    — checkout started / abandoned / completed (funnel data)
raw-inventory    — stock level changes, low-stock triggers
raw-refunds      — refund requested: amount, reason code
```

**New definition of "normal vs anomalous"** — different signals than payments:

- Checkout abandonment rate spike → broken checkout page or payment
  gateway issue
- A SKU's inventory hitting zero unexpectedly → overselling risk
- Refund rate spiking for one product → quality issue or fraud
- Order volume from one IP/customer spiking → bot or fraud attempt

**New dirty-data scenarios to test Silver layer cleaning against:**

- Negative inventory counts (overselling bugs)
- Orders with $0 total (discount code abuse)
- Duplicate order webhooks (Shopify is known to occasionally redeliver
  webhooks — idempotency handling required)
- Orders referencing deleted or non-existent products

### What would NOT change

- Kafka topic-per-event-type pattern (ADR 002)
- Bronze/Silver/Gold medallion architecture (ADR 006)
- Statistical baseline approach for anomaly detection — mean + 2 sigma
  per entity, just computed on order/checkout/inventory metrics instead
  of payment failure rates (ADR 008)
- Partition key strategy pattern — would partition by shop_id or
  customer_id depending on the topic, following the same reasoning
  process as ADRs 001, 003, 004

### Architecture sketch

```
Shopify store (webhooks: orders, checkouts, refunds, inventory)
        ↓
Shopify connector (thin layer — OAuth, webhook receiver, event translation)
        ↓
Kafka (raw-orders, raw-checkouts, raw-inventory, raw-refunds)
        ↓
Bronze → Silver → Gold              ← unchanged from core engine
        ↓
Statistical baselines + anomaly flags ← unchanged from core engine
```

Only the top two layers (Shopify store, connector) are new. Everything
from Kafka downward is the exact engine built for the portfolio project.

### When to revisit

After the Meridian roadmap is complete. At that point this becomes a legitimate side-project or product idea worth evaluating on its own merits — market demand, Shopify App Store economics, competition from existing apps (e.g. fraud/analytics apps already in the Shopify ecosystem).
