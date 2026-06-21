from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, date_trunc

# ---------------------------------------------------------------------------
# Meridian — Gold layer, user events pipeline
# Reads from Silver and produces hourly funnel metrics per device.
# The business question: how many users complete the funnel,
# and where do they drop off?
# ---------------------------------------------------------------------------

SILVER_PATH = "./data/lakehouse/silver/user_events"
GOLD_PATH = "./data/lakehouse/gold/user_events_hourly"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("meridian-gold-user-events")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )


def event_count(event_type):
    """Count rows where event_type matches the given value."""
    return count(when(col("event_type") == event_type, 1))


def main():
    print("Meridian — Gold user events pipeline starting...")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    silver_df = spark.read.format("delta").load(SILVER_PATH)
    print(f"Silver records: {silver_df.count():,}")

    hourly_df = silver_df.withColumn(
        "hour", date_trunc("hour", col("event_timestamp"))
    )

    gold_df = (
        hourly_df
        .groupBy("hour", "device")
        .agg(
            count("*").alias("total_events"),
            event_count("page_view").alias("page_views"),
            event_count("add_to_cart").alias("add_to_carts"),
            event_count("checkout").alias("checkouts"),
            event_count("purchase").alias("purchases"),
        )
        .orderBy("hour", "device")
    )

    print(f"\nWriting Gold funnel metrics to: {GOLD_PATH}")
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )

    print("\n--- Gold layer: hourly funnel by device ---")
    gold_df.show(20, truncate=False)


if __name__ == "__main__":
    main()