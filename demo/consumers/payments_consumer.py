import json

from confluent_kafka import Consumer, KafkaError

# ---------------------------------------------------------------------------
# Meridian demo consumer - payments
# Reads payment events from the raw-payments topic and prints them.
# This is NOT part of the Meridian platform itself.
# It exists to verify events are flowing through Kafka correctly.
# ---------------------------------------------------------------------------

# The Kafka broker to connect to
# localhost:9092 because Kafka is running in Docker with that port exposed
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# The topic this producer reads from
TOPIC = "raw-payments"


# Consumer configuration
consumer = Consumer({
    # Where Kafka is running
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

    # Consumer group ID - Kafka uses this to track what this
    # consumer has already read. If you stop and restart this
    # consumer it picks up where it left off, not from the beginning.
    "group.id": "meridian-payments-consumer",

    # What to do when this consumer group has no committed offset yet
    # "earliest" means start from the very first message in the topic
    # "latest" would mean only read new messages from now onwards
    "auto.offset.reset": "earliest",
})

# Subscribe to the topic
consumer.subscribe([TOPIC])

print("Meridian — payment event consumer running")
print("Waiting for events... (Ctrl+C to stop)\n")

try:
    while True:
        # Poll Kafka for new messages
        # 1.0 = wait up to 1 second for a message before trying again
        msg = consumer.poll(1.0)

        # No message arrived in the last second - just try again
        if msg is None:
            continue

        # Something went wrong reading the message
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Reached the end of the partition - not an error
                # just means we've caught up to the latest message
                print(f"  End of partition {msg.partition()}")
            else:
                print(f"  Error: {msg.error()}")
            continue

        # Parse the event
        event = json.loads(msg.value().decode("utf-8"))

        print(
            f"← partition={msg.partition()} "
            f"offset={msg.offset()} | "
            f"{event['merchant_id']} | "
            f"${event['amount']} {event['currency']} | "
            f"{event['status']}"
        )

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    # Always close cleanly - this commits final offsets to Kafka
    # so next time this consumer starts it knows where to resume
    consumer.close()