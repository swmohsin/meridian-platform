# Meridian

> Know what's wrong before your customers do.

Meridian is a real-time event intelligence platform that ingests 
high-volume data streams, detects anomalies, and uses AI agents 
to investigate and explain what happened — before your customers notice.

## What's Working

- ✅ Kafka event streaming (KRaft mode, no Zookeeper)
- ✅ Synthetic payment event producer
- ✅ Consumer with offset tracking
- 🔜 Spark lakehouse pipeline
- 🔜 AI anomaly detection agents
- 🔜 OpenTelemetry observability

## Architecture

```
[ Producers ] → [ Kafka ] → [ Spark / Flink ] → [ Delta Lake ] → [ AI Agents ] → [ API ]
```

## Run Locally

**Prerequisites:** Docker Desktop, Python 3.11+

**1. Start Kafka:**
```bash
docker compose up -d
```

**2. Create a virtual environment:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install confluent-kafka
```

**3. Run the payments producer:**
```bash
python demo/producers/payments_producer.py
```

**4. In a second terminal, run the consumer:**
```bash
source .venv/bin/activate
python demo/consumers/payments_consumer.py
```

You should see live payment events flowing between producer and consumer.

## Tech Stack

- **Streaming:** Apache Kafka (KRaft mode)
- **Infrastructure:** Docker Compose
- **Coming:** Apache Spark, Delta Lake, Apache Flink, LangGraph, OpenTelemetry, AWS

## License

Apache 2.0
