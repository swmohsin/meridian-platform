from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Meridian — Silver layer, user events pipeline
# Reads from Bronze Delta Lake table and produces a clean, validated,
# deduplicated dataset of user activity.
#
# Same pattern as payments_silver.py — null check, business rule
# validation, deduplication, timestamp parsing — applied to a
# different event shape.
# ---------------------------------------------------------------------------

BRONZE_PATH = "./data/lakehouse/bronze/user_events"
SILVER_PATH = "./data/lakehouse/silver/user_events"

VALID_EVENT_TYPES = [
    "page_view", "click", "signup", "add_to_cart", "checkout", "purchase"
]
VALID_DEVICES = ["mobile", "desktop", "tablet"]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("meridian-silver-user-events")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    print("Meridian — Silver user events pipeline starting...")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    total_bronze = bronze_df.count()
    print(f"\nBronze records: {total_bronze:,}")

    # Step 1 — null check on critical fields
    clean_df = bronze_df.filter(
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("event_type").isNotNull()
    )
    after_nulls = clean_df.count()
    print(f"After null removal: {after_nulls:,} "
          f"(dropped {total_bronze - after_nulls:,})")

    # Step 2 — business rule validation
    # event_type and device must be known values
    clean_df = clean_df.filter(
        col("event_type").isin(VALID_EVENT_TYPES) &
        col("device").isin(VALID_DEVICES)
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