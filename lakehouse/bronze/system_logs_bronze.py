from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Meridian — Bronze layer, system logs pipeline
# Reads raw service health events from Kafka and writes to Delta Lake.
# Stores error rates, latency, CPU and memory metrics per service.
# ---------------------------------------------------------------------------

SYSTEM_LOG_SCHEMA = StructType([
    StructField("event_id",        StringType(), True),
    StructField("service",         StringType(), True),
    StructField("level",           StringType(), True),
    StructField("message",         StringType(), True),
    StructField("latency_ms",      DoubleType(), True),
    StructField("cpu_percent",     DoubleType(), True),
    StructField("memory_percent",  DoubleType(), True),
    StructField("timestamp",       StringType(), True),
])

BRONZE_PATH = "./data/lakehouse/bronze/system_logs"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-system-logs"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("meridian-bronze-system-logs")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    print("Meridian — Bronze system logs pipeline starting...")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
    print(f"Writing to Delta Lake: {BRONZE_PATH}")
    print("Press Ctrl+C to stop\n")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        kafka_stream
        .select(
            from_json(
                col("value").cast("string"),
                SYSTEM_LOG_SCHEMA
            ).alias("data"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "partition", "offset", "kafka_timestamp")
    )

    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation",
                "./data/checkpoints/bronze/system_logs")
        .start(BRONZE_PATH)
    )

    print("Pipeline running. Writing system logs to Delta Lake...")
    query.awaitTermination()


if __name__ == "__main__":
    main()