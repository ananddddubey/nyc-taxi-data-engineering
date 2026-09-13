"""
Stage 2 - Silver layer.

Cleans bronze data, derives analytical fields, joins to the taxi zone
lookup, and separates rejected records instead of silently dropping them.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.spark_utils import load_config, get_spark


def load_zone_lookup(spark, path: str):
    return (
        spark.read.option("header", True).csv(path)
        .withColumnRenamed("LocationID", "location_id")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone")
        .withColumnRenamed("service_zone", "service_zone")
    )


def build_quality_flags(df):
    return df.withColumn(
        "is_valid",
        (F.col("trip_distance") > 0)
        & (F.col("fare_amount") >= 0)
        & (F.col("total_amount") >= 0)
        & (F.col("passenger_count") > 0)
        & F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & (F.col("dropoff_datetime") >= F.col("pickup_datetime"))
        & F.col("pu_location_id").isNotNull()
        & F.col("do_location_id").isNotNull(),
    )


def dedupe(df):
    # Same trip re-ingested twice (e.g. an overlapping monthly file) -
    # keep the first occurrence by ingestion time.
    key_cols = ["vendor_id", "pickup_datetime", "dropoff_datetime", "pu_location_id", "total_amount"]
    w = Window.partitionBy(*key_cols).orderBy("ingestion_timestamp")
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def derive_fields(df):
    return (
        df.withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0,
        )
        .withColumn(
            "trip_speed_mph",
            F.when(F.col("trip_duration_minutes") > 0,
                   F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0))
             .otherwise(F.lit(None)),
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_day", F.dayofmonth("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        .withColumn("pickup_year", F.year("pickup_datetime"))
        .withColumn("is_weekend", F.dayofweek("pickup_datetime").isin(1, 7))
    )


def join_zones(df, zones):
    pu = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    do = zones.select(
        F.col("location_id").alias("do_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )
    return df.join(F.broadcast(pu), "pu_location_id", "left").join(F.broadcast(do), "do_location_id", "left")


def run():
    cfg = load_config()
    spark = get_spark(cfg)

    bronze = spark.read.parquet(cfg["paths"]["bronze_dir"])
    bronze = bronze.withColumn(
        "pu_location_id", F.col("pu_location_id").cast("int")
    ).withColumn(
        "do_location_id", F.col("do_location_id").cast("int")
    )

    flagged = build_quality_flags(bronze)
    total = flagged.count()

    valid = flagged.filter("is_valid").drop("is_valid")
    rejected = flagged.filter("NOT is_valid")

    valid = dedupe(valid)
    valid = derive_fields(valid)

    zones = load_zone_lookup(spark, cfg["paths"]["zone_lookup"])
    valid = join_zones(valid, zones)

    valid_count = valid.count()
    rejected_count = total - valid_count

    (
        valid.write.mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(cfg["paths"]["silver_dir"])
    )
    rejected.write.mode("overwrite").parquet(cfg["paths"]["silver_rejected_dir"])

    print("Data Quality Report")
    print("-------------------")
    print(f"Total records:     {total:,}")
    print(f"Valid records:     {valid_count:,}")
    print(f"Rejected records:  {rejected_count:,}")
    print(f"Reject rate:       {rejected_count / total:.2%}" if total else "n/a")

    spark.stop()


if __name__ == "__main__":
    run()
