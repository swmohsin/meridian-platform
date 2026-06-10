import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Meridian demo data generator - system logs
# Simulates internal service health metrics: error rates, latency,
# CPU usage. This is the stream that tells you your infrastructure
# is struggling before your users notice.
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "raw-system-logs"

SERVICES = [
    "payments-service",
    "user-service",
    "inventory-service",
    "notification-service",
]

LOG_LEVELS = [
    "INFO",     # 5 out of 8 = normal operations
    "INFO",
    "INFO",
    "INFO",
    "INFO",
    "WARN",     # 2 out of 8 = something worth watching
    "WARN",
    "ERROR",    # 1 out of 8 = something is wrong
]

MESSAGES = {
    "INFO": [
        "Request processed successfully",
        "Database connection healthy",
        "Cache hit ratio 98%",
        "Heartbeat OK",
    ],
    "WARN": [
        "Response time elevated: 450ms",
        "Database connection pool at 80% capacity",
        "Retry attempt 1 of 3",
        "Cache miss ratio increasing",
    ],
    "ERROR": [
        "Database connection timeout",
        "Payment gateway unreachable",
        "Request failed after 3 retries",
        "Memory usage critical: 94%",
    ],
}


def generate_system_log():
    """Generate one realistic system log event."""
    service = random.choice(SERVICES)
    level = random.choice(LOG_LEVELS)
    return {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "service": service,
        "level": level,
        "message": random.choice(MESSAGES[level]),
        "latency_ms": round(random.uniform(10, 2000), 2),
        "cpu_percent": round(random.uniform(10, 95), 1),
        "memory_percent": round(random.uniform(20, 95), 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"  ✗ Delivery failed: {err}")
    else:
        print(f"  ✓ Delivered to partition {msg.partition()} offset {msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    print("Meridian — system logs producer running")
    print(f"Writing to topic: {TOPIC}")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            event = generate_system_log()

            # Key by service name — all logs for one service
            # go to the same partition, preserving order per service
            producer.produce(
                topic=TOPIC,
                key=event["service"],
                value=json.dumps(event),
                callback=delivery_report,
            )

            producer.flush()

            # Emoji makes ERROR logs visually obvious in the terminal
            indicator = "🔴" if event["level"] == "ERROR" else "🟡" if event["level"] == "WARN" else "🟢"

            print(
                f"{indicator} {event['service']} | "
                f"{event['level']} | "
                f"{event['message']} | "
                f"latency={event['latency_ms']}ms"
            )

            # System logs come every 0.5 seconds
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nGenerator stopped.")

if __name__ == "__main__":
    main()