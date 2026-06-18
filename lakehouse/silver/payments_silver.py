from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Meridian — Silver layer, payments pipeline
# Reads from Bronze Delta Lake table and produces a clean, validated,
# deduplicated dataset. This is the first layer safe for AI and analytics.
#
# Bronze → stores everything raw, no questions asked
# Silver → cleans, validates, deduplicates
# ---------------------------------------------------------------------------

BRONZE_PATH = "./data/lakehouse/bronze/payments"
SILVER_PATH = "./data/lakehouse/silver/payments"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("meridian-silver-payments")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    print("Meridian — Silver payments pipeline starting...")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Read from Bronze — this is a batch read, not streaming
    # Silver processes what Bronze has accumulated
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    total_bronze = bronze_df.count()
    print(f"\nBronze records: {total_bronze:,}")

    # ------------------------------------------------------------------
    # Step 1 — Remove nulls
    # Any record missing critical fields is unusable
    # ------------------------------------------------------------------
    clean_df = bronze_df.filter(
        col("event_id").isNotNull() &
        col("merchant_id").isNotNull() &
        col("amount").isNotNull() &
        col("status").isNotNull()
    )

    after_nulls = clean_df.count()
    print(f"After null removal: {after_nulls:,} "
          f"(dropped {total_bronze - after_nulls:,})")

    # ------------------------------------------------------------------
    # Step 2 — Validate business rules
    # Amounts must be positive
    # Status must be one of the known values
    # ------------------------------------------------------------------
    valid_statuses = ["completed", "failed", "pending"]

    clean_df = clean_df.filter(
        (col("amount") > 0) &
        (col("currency").isin(["USD", "CAD", "EUR", "GBP"])) &
        (col("status").isin(valid_statuses))
    )

    after_validation = clean_df.count()
    print(f"After validation: {after_validation:,} "
          f"(dropped {after_nulls - after_validation:,})")

    # ------------------------------------------------------------------
    # Step 3 — Remove duplicates
    # Keep only the latest record per event_id
    # Duplicates can happen if the Bronze pipeline reprocessed events
    # ------------------------------------------------------------------
    window = Window.partitionBy("event_id").orderBy(
        col("kafka_timestamp").desc()
    )

    deduped_df = (
        clean_df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    after_dedup = deduped_df.count()
    print(f"After deduplication: {after_dedup:,} "
          f"(dropped {after_validation - after_dedup:,})")

    # ------------------------------------------------------------------
    # Step 4 — Parse timestamp properly
    # Bronze stores timestamp as string — Silver converts to proper type
    # ------------------------------------------------------------------
    silver_df = deduped_df.withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp"))
    ).drop("timestamp")

    # ------------------------------------------------------------------
    # Step 5 — Write to Silver Delta Lake table
    # ------------------------------------------------------------------
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
    print(f"Total Bronze records:     {total_bronze:,}")
    print(f"Clean Silver records:     {after_dedup:,}")
    print(f"Records dropped:          {total_bronze - after_dedup:,}")
    print(f"Data quality score:       "
          f"{(after_dedup/total_bronze*100):.1f}%")


if __name__ == "__main__":
    main()