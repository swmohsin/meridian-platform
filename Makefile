# Meridian - development commands
# Run 'make help' to see all available commands

.PHONY: help run stop status topics producers

# Default target when you just type 'make'
help:
	@echo ""
	@echo "Meridian — Know what's wrong before your customers do."
	@echo ""
	@echo "Usage:"
	@echo "  make run			Start Kafka"
	@echo "  make stop      		Stop Kafka"
	@echo "  make status     		Show running containers"
	@echo "  make topics    		Create all Kafka topics"
	@echo "  make producers  		Run all three producers simultaneously"
	@echo "  make dirty-payments-producer	Run dirty payments data producer for testing Silver"
	@echo "  make stop-producers		Stop all running producers"
	@echo "  make consume         		Watch unified event stream"
	@echo "  make bronze-payments      	Run Bronze payments pipeline"
	@echo "  make bronze-user-events   	Run Bronze user events pipeline"
	@echo "  make bronze-system-logs   	Run Bronze system logs pipeline"
	@echo "  make silver-payments		Run Silver payments pipeline"
	@echo "  make silver-user-events  	Run Silver user events pipeline"
	@echo "  make gold-payments		Run Gold payments pipeline"
	@echo "  make gold-user-events    	Run Gold user events pipeline"

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
	@$(MAKE) topics
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

# Stop all running producers
stop-producers:
	@echo "Stopping all producers..."
	@pkill -f payments_producer.py || true
	@pkill -f user_events_producer.py || true
	@pkill -f system_logs_producer.py || true
	@echo "Producers stopped"

consume:
	@source .venv/bin/activate && python demo/consumers/unified_consumer.py

bronze-payments:
	@echo "Starting Bronze payments pipeline — Ctrl+C to stop"
	@.venv/bin/python lakehouse/bronze/payments_bronze.py

bronze-user-events:
	@echo "Starting Bronze user events pipeline — Ctrl+C to stop"
	@.venv/bin/python lakehouse/bronze/user_events_bronze.py

bronze-system-logs:
	@echo "Starting Bronze system logs pipeline — Ctrl+C to stop"
	@.venv/bin/python lakehouse/bronze/system_logs_bronze.py

silver-payments:	
	@echo "Running Silver payments pipeline..."
	@.venv/bin/python lakehouse/silver/payments_silver.py

dirty-payments-producer:
	@echo "Starting dirty payments producer..."
	@.venv/bin/python demo/producers/dirty_payments_producer.py

gold-payments:
	@echo "Running Gold payments pipeline..."
	@.venv/bin/python lakehouse/gold/payments_gold.py

	silver-user-events:
	@echo "Running Silver user events pipeline..."
	@.venv/bin/python lakehouse/silver/user_events_silver.py

gold-user-events:
	@echo "Running Gold user events pipeline..."
	@.venv/bin/python lakehouse/gold/user_events_gold.py