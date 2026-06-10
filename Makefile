# Meridian - development commands
# Run 'make help' to see all available commands

.PHONY: help run stop status topics producers

# Default target when you just type 'make'
help:
	@echo ""
	@echo "Meridian — Know what's wrong before your customers do."
	@echo ""
	@echo "Usage:"
	@echo "  make run         Start Kafka"
	@echo "  make stop        Stop Kafka"
	@echo "  make status      Show running containers"
	@echo "  make topics      Create all Kafka topics"
	@echo "  make producers   Run all three producers simultaneously"
	@echo ""

# Start Kafka
run:
	docker compose up -d
	@echo "Kafka is running on localhost:9092"

# Stop Kafka
stop:
	docker compose down
	@echo "Kafka stopped"

# Show running containers
status:
	docker ps --filter "name=meridian"

# Create all topics with correct partition counts
# Safe to run multiple times - skips topics that already exist
topics:
	@echo "Creating Meridian topics..."
	-docker exec meridian-kafka kafka-topics \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic raw-payments \
		--partitions 3
	-docker exec meridian-kafka kafka-topics \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic raw-user-events \
		--partitions 3
	-docker exec meridian-kafka kafka-topics \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic raw-system-logs \
		--partitions 3
	@echo "Topics ready"
	docker exec meridian-kafka kafka-topics \
		--bootstrap-server localhost:9092 \
		--list

# Run all three producers simultaneously
# Each runs in the background, logs go to logs/ folder
producers:
	@mkdir -p logs
	@echo "Starting all producers..."
	@source .venv/bin/activate && \
		python demo/producers/payments_producer.py > logs/payments.log 2>&1 & \
		python demo/producers/user_events_producer.py > logs/user_events.log 2>&1 & \
		python demo/producers/system_logs_producer.py > logs/system_logs.log 2>&1 &
	@echo "All producers running in background"
	@echo "Tail logs with:"
	@echo "  tail -f logs/payments.log"
	@echo "  tail -f logs/user_events.log"
	@echo "  tail -f logs/system_logs.log"