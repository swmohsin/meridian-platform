import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Meridian demo data generator - user events
# Simulates user activity on a platform: page views, clicks,
# signups, cart actions, and checkouts.
# In production, replace this with your real analytics event stream.
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "raw-user-events"

PAGES = [
    "/home",
    "/products",
    "/product/123",
    "/cart",
    "/checkout",
    "/confirmation",
    "/account",
]

EVENT_TYPES = [
    "page_view",    # user viewed a page
    "page_view",    # 3 out of 9 = 33% chance
    "page_view",
    "click",        # user clicked something
    "click",        # 2 out of 9 = 22% chance
    "signup",       # new user registered
    "add_to_cart",  # user added item to cart
    "checkout",     # user started checkout
    "purchase",     # user completed purchase
]

DEVICES = ["mobile", "desktop", "tablet"]


def generate_user_event():
    """Generate one realistic synthetic user event."""
    event_type = random.choice(EVENT_TYPES)
    return {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "user_id": f"user_{random.randint(1, 500)}",
        "session_id": f"session_{random.randint(1000, 9999)}",
        "event_type": event_type,
        "page": random.choice(PAGES),
        "device": random.choice(DEVICES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"  ✗ Delivery failed: {err}")
    else:
        print(f"  ✓ Delivered to partition {msg.partition()} offset {msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    print("Meridian — user event producer running")
    print(f"Writing to topic: {TOPIC}")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            event = generate_user_event()

            # Key by user_id — all events for one user
            # go to the same partition, preserving their journey order
            producer.produce(
                topic=TOPIC,
                key=event["user_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )

            producer.flush()

            print(f"→ {event['user_id']} | {event['event_type']} | {event['page']} | {event['device']}")

            # User events happen faster than payments
            time.sleep(0.3)

    except KeyboardInterrupt:
        print("\nGenerator stopped.")


if __name__ == "__main__":
    main()