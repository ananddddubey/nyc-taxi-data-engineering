"""
Stage 3 - Gold layer.

Builds the star schema: dim_date, dim_location, dim_payment, dim_vendor,
fact_trips. Writes each as Parquet; optionally loads to Postgres for the
SQL layer / Power BI to sit on top of.
"""
from pyspark.sql import functions as F

from src.utils.spark_utils import load_config, get_spark

PAYMENT_TYPES = {
    1: "Credit card", 2: "Cash", 3: "No charge",
    4: "Dispute", 5: "Unknown", 6: "Voided trip",
}
VENDORS = {
    1: "Creative Mobile Technologies, LLC",
    2: "VeriFone Inc.",
}


def build_dim_date(spark, silver):
    dates = silver.select(F.col("pickup_date").alias("full_date")).distinct()
    return (
        dates.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("week", F.weekofyear("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
    )


def build_dim_location(spark, zone_lookup_path):
    zones = spark.read.option("header", True).csv(zone_lookup_path)
    return zones.select(
        F.col("LocationID").cast("int").alias("location_key"),
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
    )


def build_dim_payment(spark):
    rows = [(k, v) for k, v in PAYMENT_TYPES.items()]
    return spark.createDataFrame(rows, ["payment_key", "payment_description"]).withColumn(
        "payment_type", F.col("payment_key")
    )


def build_dim_vendor(spark):
    rows = [(k, v) for k, v in VENDORS.items()]
    return spark.createDataFrame(rows, ["vendor_key", "vendor_name"]).withColumn(
        "vendor_id", F.col("vendor_key")
    )


def build_fact_trips(silver):
    return silver.select(
        F.monotonically_increasing_id().alias("trip_id"),
        F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("pu_location_id").alias("pickup_location_key"),
        F.col("do_location_id").alias("dropoff_location_key"),
        F.col("vendor_id").alias("vendor_key"),
        F.col("payment_type").alias("payment_key"),
        F.col("rate_code_id").alias("rate_code_key"),
        "passenger_count",
        "trip_distance",
        "trip_duration_minutes",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone",
    )


def run():
    cfg = load_config()
    spark = get_spark(cfg)

    silver = spark.read.parquet(cfg["paths"]["silver_dir"])

    dim_date = build_dim_date(spark, silver)
    dim_location = build_dim_location(spark, cfg["paths"]["zone_lookup"])
    dim_payment = build_dim_payment(spark)
    dim_vendor = build_dim_vendor(spark)
    fact_trips = build_fact_trips(silver)

    gold_dir = cfg["paths"]["gold_dir"]
    dim_date.write.mode("overwrite").parquet(f"{gold_dir}/dim_date")
    dim_location.write.mode("overwrite").parquet(f"{gold_dir}/dim_location")
    dim_payment.write.mode("overwrite").parquet(f"{gold_dir}/dim_payment")
    dim_vendor.write.mode("overwrite").parquet(f"{gold_dir}/dim_vendor")
    fact_trips.write.mode("overwrite").partitionBy("date_key").parquet(f"{gold_dir}/fact_trips")

    print(f"Gold layer written to {gold_dir}")
    print(f"fact_trips row count: {fact_trips.count():,}")

    spark.stop()


if __name__ == "__main__":
    run()
