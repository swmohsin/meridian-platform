# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, round as spark_round

# ---------------------------------------------------------------------------
# Meridian — Baseline calculator for payments
# Computes the "normal" failure rate per merchant from historical Gold data.
# This baseline is what future anomaly detection compares against.
#
# The core question: how do we know something is wrong?
# Answer: when it deviates significantly from this merchant's own history.
# ---------------------------------------------------------------------------

GOLD_PATH = "./data/lakehouse/gold/payments_hourly"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("meridian-payments-baseline")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    print("Meridian — computing payment failure rate baselines...")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    gold_df = spark.read.format("delta").load(GOLD_PATH)

    # For each merchant: what is their normal failure rate,
    # and how much does it normally vary hour to hour?
    baseline_df = (
        gold_df
        .groupBy("merchant_id")
        .agg(
            spark_round(avg("failure_rate_pct"), 2).alias("avg_failure_rate"),
            spark_round(stddev("failure_rate_pct"), 2).alias("stddev_failure_rate"),
        )
    )

    print("\n--- Per-merchant failure rate baseline ---")
    baseline_df.show(truncate=False)

    # The anomaly threshold: average + 2 standard deviations
    # This is a standard statistical approach — anything beyond
    # 2 stddev from the mean is unusual enough to flag
    anomaly_thresholds = baseline_df.withColumn(
        "anomaly_threshold_pct",
        spark_round(
            col("avg_failure_rate") + (2 * col("stddev_failure_rate")), 2
        )
    )

    print("--- Anomaly thresholds (avg + 2 std deviations) ---")
    anomaly_thresholds.show(truncate=False)

    # Now show which actual hours crossed that threshold
    flagged = (
        gold_df.join(anomaly_thresholds, "merchant_id")
        .filter(col("failure_rate_pct") > col("anomaly_threshold_pct"))
        .select("merchant_id", "hour", "failure_rate_pct",
                "anomaly_threshold_pct", "total_transactions")
        .orderBy(col("failure_rate_pct").desc())
    )

    print("--- Hours that exceeded the anomaly threshold ---")
    flagged.show(20, truncate=False)


if __name__ == "__main__":
    main()