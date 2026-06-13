from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Meridian — Bronze layer, payments pipeline
# Reads raw payment events from Kafka and writes them to Delta Lake.
# This is the first permanent storage layer in Meridian.
# Raw events land here exactly as produced — no transformation,
# no filtering, no business logic. Just reliable persistence.
# ---------------------------------------------------------------------------

# Schema of the payment events your producer sends
# Spark needs to know the structure to parse the JSON
PAYMENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),  True),
    StructField("merchant_id", StringType(),  True),
    StructField("user_id",     StringType(),  True),
    StructField("amount",      DoubleType(),  True),
    StructField("currency",    StringType(),  True),
    StructField("status",      StringType(),  True),
    StructField("timestamp",   StringType(),  True),
])

# Where Delta Lake will store the Bronze payments table
BRONZE_PATH = "./data/lakehouse/bronze/payments"

# Kafka connection
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-payments"


def create_spark_session():
    """
    Create a Spark session with Delta Lake support.
    The extra config tells Spark to use Delta Lake
    as the catalog and enables Delta-specific SQL commands.
    """
    return (
        SparkSession.builder
        .appName("meridian-bronze-payments")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Run Spark locally using all available CPU cores
        .master("local[*]")
        .getOrCreate()
    )


def main():
    print("Meridian — Bronze payments pipeline starting...")

    spark = create_spark_session()

    # Suppress verbose Spark logs so you can see Meridian output
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
    print(f"Writing to Delta Lake: {BRONZE_PATH}")
    print("Press Ctrl+C to stop\n")

    # Read from Kafka as a streaming DataFrame
    # Spark treats the Kafka stream like a table that grows forever
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        # Start from the earliest available message
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka delivers messages as raw bytes
    # Cast the value to string first, then parse the JSON
    parsed = (
        kafka_stream
        .select(
            # Parse the JSON value using our schema
            from_json(
                col("value").cast("string"),
                PAYMENT_SCHEMA
            ).alias("data"),
            # Keep Kafka metadata — useful for debugging
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        # Flatten the nested 'data' struct into top-level columns
        .select("data.*", "partition", "offset", "kafka_timestamp")
    )

    # Write the parsed stream to Delta Lake
    # outputMode "append" means new rows are added, never updated
    # checkpointLocation tracks which Kafka offsets have been written
    # so if this job restarts it picks up where it left off
    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "./data/checkpoints/bronze/payments")
        .start(BRONZE_PATH)
    )

    print("Pipeline running. Writing events to Delta Lake...")

    # Keep the job running until Ctrl+C
    query.awaitTermination()


if __name__ == "__main__":
    main()