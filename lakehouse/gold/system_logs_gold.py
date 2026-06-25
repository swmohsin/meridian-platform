from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, avg, max as spark_max,
    date_trunc, round as spark_round
)

# ---------------------------------------------------------------------------
# Meridian — Gold layer, system logs pipeline
# Reads from Silver and produces hourly service health metrics.
# The business question: which services are unhealthy, and when?
# ---------------------------------------------------------------------------

SILVER_PATH = "./data/lakehouse/silver/system_logs"
GOLD_PATH = "./data/lakehouse/gold/system_logs_hourly"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("meridian-gold-system-logs")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )


def level_count(level):
    """Count rows where log level matches the given value."""
    return count(when(col("level") == level, 1))


def main():
    print("Meridian — Gold system logs pipeline starting...")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    silver_df = spark.read.format("delta").load(SILVER_PATH)
    print(f"Silver records: {silver_df.count():,}")

    hourly_df = silver_df.withColumn(
        "hour", date_trunc("hour", col("event_timestamp"))
    )

    gold_df = (
        hourly_df
        .groupBy("service", "hour")
        .agg(
            count("*").alias("total_logs"),
            level_count("INFO").alias("info_count"),
            level_count("WARN").alias("warn_count"),
            level_count("ERROR").alias("error_count"),
            spark_round(avg("latency_ms"), 2).alias("avg_latency_ms"),
            spark_round(spark_max("latency_ms"), 2).alias("max_latency_ms"),
            spark_round(avg("cpu_percent"), 2).alias("avg_cpu_percent"),
        )
        .withColumn(
            "error_rate_pct",
            spark_round(col("error_count") / col("total_logs") * 100, 2)
        )
        .orderBy("hour", "service")
    )

    print(f"\nWriting Gold service health metrics to: {GOLD_PATH}")
    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )

    print("\n--- Gold layer: hourly service health ---")
    gold_df.show(20, truncate=False)

    print("\n--- Services with highest error rate ---")
    gold_df.orderBy(col("error_rate_pct").desc()).show(5, truncate=False)


if __name__ == "__main__":
    main()