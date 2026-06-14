from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Meridian — Bronze layer, user events pipeline
# Reads raw user activity events from Kafka and writes to Delta Lake.
# Stores page views, clicks, signups, cart actions, and purchases.
# ---------------------------------------------------------------------------

USER_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(), True),
    StructField("user_id",     StringType(), True),
    StructField("session_id",  StringType(), True),
    StructField("event_type",  StringType(), True),
    StructField("page",        StringType(), True),
    StructField("device",      StringType(), True),
    StructField("timestamp",   StringType(), True),
])

BRONZE_PATH = "./data/lakehouse/bronze/user_events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-user-events"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("meridian-bronze-user-events")
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
    print("Meridian — Bronze user events pipeline starting...")

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
                USER_EVENT_SCHEMA
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
                "./data/checkpoints/bronze/user_events")
        .start(BRONZE_PATH)
    )

    print("Pipeline running. Writing user events to Delta Lake...")
    query.awaitTermination()


if __name__ == "__main__":
    main()