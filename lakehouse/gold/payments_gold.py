from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, round as spark_round,
    date_trunc, when
)

# ---------------------------------------------------------------------------
# Meridian — Gold layer, payments pipeline
# Reads from Silver and produces business-ready aggregates.
# These are the numbers Meridian's anomaly detection will watch.
# ---------------------------------------------------------------------------

SILVER_PATH = "./data/lakehouse/silver/payments"
GOLD_PATH = "./data/lakehouse/gold/payments_hourly"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("meridian-gold-payments")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    print("Meridian — Gold payments pipeline starting...")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    silver_df = spark.read.format("delta").load(SILVER_PATH)
    print(f"Silver records: {silver_df.count():,}")

    # Bucket every event into its hour for aggregation
    hourly_df = silver_df.withColumn(
        "hour", date_trunc("hour", col("event_timestamp"))
    )

    # Business metric: per merchant, per hour
    # - total transactions
    # - completed vs failed counts
    # - failure rate (the number Meridian watches for anomalies)
    # - total and average completed volume
    gold_df = (
        hourly_df
        .groupBy("merchant_id", "hour")
        .agg(
            count("*").alias("total_transactions"),
            spark_sum(when(col("status") == "completed", 1).otherwise(0))
                .alias("completed_count"),
            spark_sum(when(col("status") == "failed", 1).otherwise(0))
                .alias("failed_count"),
            spark_sum(when(col("status") == "completed", col("amount")).otherwise(0))
                .alias("completed_volume"),
        )
        .withColumn(
            "failure_rate_pct",
            spark_round(col("failed_count") / col("total_transactions") * 100, 2)
        )
        .orderBy("hour", "merchant_id")
    )

    print(f"\nWriting Gold aggregates to: {GOLD_PATH}")

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )

    print("\n--- Gold layer: merchant hourly metrics ---")
    gold_df.show(20, truncate=False)

    print("\n--- Merchants with highest failure rate ---")
    gold_df.orderBy(col("failure_rate_pct").desc()).show(5, truncate=False)


if __name__ == "__main__":
    main()