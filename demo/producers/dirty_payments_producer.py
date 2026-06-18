import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Meridian demo — dirty payment event producer
# Intentionally generates bad data to test Silver layer cleaning.
# Produces: nulls, invalid amounts, unknown statuses, bad currencies,
# duplicates, and missing fields.
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "raw-payments"

MERCHANTS = ["merchant_001", "merchant_002", "merchant_003", "merchant_004"]


def generate_dirty_event():
    """
    Generate one intentionally broken payment event.
    Each call randomly picks a different type of problem.
    """
    problem = random.choice([
        "null_amount",
        "negative_amount",
        "zero_amount",
        "invalid_status",
        "invalid_currency",
        "missing_merchant",
        "missing_event_id",
        "duplicate",
        "clean",           # occasionally clean to mix in
        "clean",           # weight clean higher so pipeline doesn't
        "clean",           # get overwhelmed with bad data
    ])

    base_event = {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "merchant_id": random.choice(MERCHANTS),
        "user_id": f"user_{random.randint(1, 500)}",
        "amount": round(random.uniform(5.0, 2000.0), 2),
        "currency": random.choice(["USD", "CAD", "EUR", "GBP"]),
        "status": "completed",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if problem == "null_amount":
        base_event["amount"] = None
        label = "❌ NULL amount"

    elif problem == "negative_amount":
        base_event["amount"] = round(random.uniform(-500, -1), 2)
        label = "❌ NEGATIVE amount"

    elif problem == "zero_amount":
        base_event["amount"] = 0.0
        label = "❌ ZERO amount"

    elif problem == "invalid_status":
        base_event["status"] = random.choice([
            "APPROVED", "DECLINED", "unknown", "processing", ""
        ])
        label = f"❌ INVALID status: {base_event['status']}"

    elif problem == "invalid_currency":
        base_event["currency"] = random.choice([
            "XYZ", "BTC", "USDT", "€", ""
        ])
        label = f"❌ INVALID currency: {base_event['currency']}"

    elif problem == "missing_merchant":
        del base_event["merchant_id"]
        label = "❌ MISSING merchant_id"

    elif problem == "missing_event_id":
        del base_event["event_id"]
        label = "❌ MISSING event_id"

    elif problem == "duplicate":
        # Send same event_id twice — Silver must deduplicate
        base_event["event_id"] = "evt_DUPLICATE_999"
        label = "⚠️  DUPLICATE event_id"

    else:
        label = "✅ clean"

    return base_event, label


def delivery_report(err, msg):
    if err is not None:
        print(f"  ✗ Delivery failed: {err}")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    print("Meridian — dirty payment producer running")
    print("Sending intentionally bad data to test Silver cleaning")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            event, label = generate_dirty_event()

            producer.produce(
                topic=TOPIC,
                key=event.get("merchant_id", "unknown"),
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.flush()

            print(f"{label} | {json.dumps(event)[:80]}...")
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nDirty producer stopped.")


if __name__ == "__main__":
    main()