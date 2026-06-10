import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Meridian demo data producer - payments
# This is NOT part of the Meridian platform itself.
# It produces synthetic payment events so you can run Meridian locally
# without connecting a real data source.
# In production, replace this with your own Kafka producer or Kafka Connect.
# ---------------------------------------------------------------------------

# The Kafka broker to connect to
# localhost:9092 because Kafka is running in Docker with that port exposed
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# The topic this generator writes to
TOPIC = "raw-payments"

# Sample data to make events feel realistic
MERCHANTS = [
    "merchant_001",  # coffee shop
    "merchant_002",  # online retailer
    "merchant_003",  # grocery store
    "merchant_004",  # gas station
]

STATUSES = [
    "completed",   # most payments succeed
    "completed",
    "completed",
    "failed",      # occasional failure - realistic
    "pending",     # occasionally stuck in pending
]

CURRENCIES = ["USD", "CAD", "EUR", "GBP"]


def generate_payment_event():
    """Generate one realistic synthetic payment event."""
    return {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "merchant_id": random.choice(MERCHANTS),
        "user_id": f"user_{random.randint(1, 500)}",
        "amount": round(random.uniform(5.0, 2000.0), 2),
        "currency": random.choice(CURRENCIES),
        "status": random.choice(STATUSES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    """
    Called by Kafka producer for every message to confirm delivery.
    If err is not None, the message failed to deliver.
    This is how you know your events actually reached the broker.
    """
    if err is not None:
        print(f"  ✗ Delivery failed: {err}")
    else:
        print(f"  ✓ Delivered to partition {msg.partition()} offset {msg.offset()}")


def main():
    # Connect to Kafka
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    print("Meridian — payment event producer running")
    print(f"Writing to topic: {TOPIC}")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            event = generate_payment_event()

            # Send the event to Kafka
            # key=merchant_id ensures all events for one merchant
            # go to the same partition - preserving order per merchant
            producer.produce(
                topic=TOPIC,
                key=event["merchant_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )

            # Actually send buffered messages to the broker
            producer.flush()

            print(f"→ {event['merchant_id']} | ${event['amount']} {event['currency']} | {event['status']}")

            # One event every second - easy to watch
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nGenerator stopped.")


if __name__ == "__main__":
    main()