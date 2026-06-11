import json

from confluent_kafka import Consumer, KafkaError

# ---------------------------------------------------------------------------
# Meridian unified consumer
# Reads from all three event streams simultaneously and prints
# a correlated view of everything happening across the platform.
# This is the first time payments, user activity, and system health
# appear together in one place.
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = [
    "raw-payments",
    "raw-user-events",
    "raw-system-logs",
]

# Visual indicators per topic
ICONS = {
    "raw-payments": "💳",
    "raw-user-events": "👤",
    "raw-system-logs": {
        "INFO": "🟢",
        "WARN": "🟡",
        "ERROR": "🔴",
    }
}


def format_payment(event):
    icon = ICONS["raw-payments"]
    return (
        f"{icon} PAYMENT   | "
        f"{event['merchant_id']} | "
        f"${event['amount']} {event['currency']} | "
        f"{event['status'].upper()}"
    )


def format_user_event(event):
    icon = ICONS["raw-user-events"]
    return (
        f"{icon} USER      | "
        f"{event['user_id']} | "
        f"{event['event_type']} | "
        f"{event['page']} | "
        f"{event['device']}"
    )


def format_system_log(event):
    level = event["level"]
    icon = ICONS["raw-system-logs"][level]
    return (
        f"{icon} SYSTEM    | "
        f"{event['service']} | "
        f"{event['level']} | "
        f"{event['message']} | "
        f"latency={event['latency_ms']}ms"
    )


def format_event(topic, event):
    """Route event to the correct formatter based on its topic."""
    if topic == "raw-payments":
        return format_payment(event)
    elif topic == "raw-user-events":
        return format_user_event(event)
    elif topic == "raw-system-logs":
        return format_system_log(event)


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

        # Unique group ID — separate from the payments-only consumer
        # so both can run independently without affecting each other's offsets
        "group.id": "meridian-unified-consumer",

        # Start from the beginning so we see all stored events
        "auto.offset.reset": "earliest",
    })

    # Subscribe to all three topics at once
    # Kafka handles the partition assignment automatically
    consumer.subscribe(TOPICS)

    print("Meridian — unified event stream")
    print("Watching payments, user activity, and system health")
    print("Press Ctrl+C to stop\n")
    print("-" * 70)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Error: {msg.error()}")
                continue

            # Parse the event
            event = json.loads(msg.value().decode("utf-8"))

            # Format and print based on which topic it came from
            output = format_event(msg.topic(), event)
            print(output)

    except KeyboardInterrupt:
        print("\n" + "-" * 70)
        print("Unified consumer stopped.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()