# Meridian

> Know what's wrong before your customers do.

Meridian is a real-time event intelligence platform that ingests
high-volume data streams, detects anomalies, and uses AI agents
to investigate and explain what happened — before your customers notice.

## What's Working

- ✅ Kafka event streaming (KRaft mode, no Zookeeper)
- ✅ Three synthetic event streams: payments, user activity, system health
- ✅ Unified real-time view across all three streams
- ✅ Data persisted to local volume
- ✅ Bronze layer — payments events stored permanently in Delta Lake
- ✅ Bronze layer queries — failure rates, volume by currency, status breakdown
- ✅ Silver layer — 96.1% data quality, drops nulls, invalid records, duplicates
- ✅ Gold layer — hourly merchant metrics, failure rate detection
- ✅ Full medallion architecture (Bronze/Silver/Gold) for all three streams: payments, user events, system logs
- ✅ Statistical anomaly baselines for payment failure rates
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
pip install -r requirements.txt
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
