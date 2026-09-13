"""Sample unit tests - run with: pytest tests/"""
from pyspark.sql import SparkSession
from src.transformation.silver import build_quality_flags, derive_fields


def get_spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_invalid_trip_distance_flagged():
    spark = get_spark()
    df = spark.createDataFrame(
        [(0.0, 10.0, 10.0, 1, "2019-01-01 08:00:00", "2019-01-01 08:10:00", 1, 1)],
        ["trip_distance", "fare_amount", "total_amount", "passenger_count",
         "pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id"],
    )
    df = df.withColumn("pickup_datetime", df.pickup_datetime.cast("timestamp"))
    df = df.withColumn("dropoff_datetime", df.dropoff_datetime.cast("timestamp"))
    flagged = build_quality_flags(df)
    assert flagged.collect()[0]["is_valid"] is False  # trip_distance == 0
