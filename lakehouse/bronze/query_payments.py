from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round

# ---------------------------------------------------------------------------
# Meridian — Bronze payments query script
# Ask real business questions against stored payment history.
# This is the first time Meridian answers something useful.
# ---------------------------------------------------------------------------

BRONZE_PATH = "./data/lakehouse/bronze/payments"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("meridian-query-payments")
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
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.format("delta").load(BRONZE_PATH)

    print("\n" + "="*60)
    print("MERIDIAN — Payment Intelligence Report")
    print("="*60)

    # Question 1 — how many events do we have?
    total = df.count()
    print(f"\nTotal payment events stored: {total:,}")

    # Question 2 — which merchant has the most failures?
    print("\n--- Failure rate by merchant ---")
    df.filter(col("status") == "failed") \
      .groupBy("merchant_id") \
      .agg(count("*").alias("failures")) \
      .orderBy(col("failures").desc()) \
      .show()

    # Question 3 — revenue by currency
    print("--- Total volume by currency ---")
    df.filter(col("status") == "completed") \
      .groupBy("currency") \
      .agg(
          count("*").alias("transactions"),
          round(sum("amount"), 2).alias("total_volume"),
          round(avg("amount"), 2).alias("avg_amount")
      ) \
      .orderBy(col("total_volume").desc()) \
      .show()

    # Question 4 — this is the core Meridian question
    print("--- Status breakdown per merchant ---")
    df.groupBy("merchant_id", "status") \
      .agg(count("*").alias("count")) \
      .orderBy("merchant_id", "status") \
      .show()

    print("="*60)


if __name__ == "__main__":
    main()