from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Meridian — Silver layer, system logs pipeline
# Reads from Bronze Delta Lake table and produces a clean, validated,
# deduplicated dataset of service health metrics.
#
# Same pattern as payments and user_events Silver — null check,
# business rule validation, deduplication, timestamp parsing —
# applied to a different event shape.
# ---------------------------------------------------------------------------

BRONZE_PATH = "./data/lakehouse/bronze/system_logs"
SILVER_PATH = "./data/lakehouse/silver/system_logs"

VALID_LEVELS = ["INFO", "WARN", "ERROR"]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("meridian-silver-system-logs")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    print("Meridian — Silver system logs pipeline starting...")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    total_bronze = bronze_df.count()
    print(f"\nBronze records: {total_bronze:,}")

    # Step 1 — null check on critical fields
    clean_df = bronze_df.filter(
        col("event_id").isNotNull() &
        col("service").isNotNull() &
        col("level").isNotNull() &
        col("latency_ms").isNotNull()
    )
    after_nulls = clean_df.count()
    print(f"After null removal: {after_nulls:,} "
          f"(dropped {total_bronze - after_nulls:,})")

    # Step 2 — business rule validation
    # level must be a known severity, latency/cpu/memory must be
    # physically plausible (no negative latency, percentages 0-100)
    clean_df = clean_df.filter(
        col("level").isin(VALID_LEVELS) &
        (col("latency_ms") >= 0) &
        (col("cpu_percent").between(0, 100)) &
        (col("memory_percent").between(0, 100))
    )
    after_validation = clean_df.count()
    print(f"After validation: {after_validation:,} "
          f"(dropped {after_nulls - after_validation:,})")

    # Step 3 — deduplicate by event_id, keep latest
    window = Window.partitionBy("event_id").orderBy(col("kafka_timestamp").desc())
    deduped_df = (
        clean_df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    after_dedup = deduped_df.count()
    print(f"After deduplication: {after_dedup:,} "
          f"(dropped {after_validation - after_dedup:,})")

    # Step 4 — parse timestamp
    silver_df = deduped_df.withColumn(
        "event_timestamp", to_timestamp(col("timestamp"))
    ).drop("timestamp")

    # Step 5 — write to Silver
    print(f"\nWriting {after_dedup:,} clean records to Silver layer...")
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH)
    )

    print(f"Silver layer written to: {SILVER_PATH}")
    print("\n--- Silver layer sample ---")
    silver_df.show(5, truncate=False)

    print("\n--- Data quality summary ---")
    print(f"Total Bronze records:  {total_bronze:,}")
    print(f"Clean Silver records:  {after_dedup:,}")
    print(f"Records dropped:       {total_bronze - after_dedup:,}")
    print(f"Data quality score:    {(after_dedup/total_bronze*100):.1f}%")


if __name__ == "__main__":
    main()