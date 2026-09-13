"""
Standalone data-quality checks, runnable independently of the pipeline
(e.g. in Airflow as its own task, or ad hoc in a notebook).
"""
from pyspark.sql import functions as F


CHECKS = {
    "null_trip_id": lambda df: df.filter(F.col("trip_id").isNull()).count(),
    "null_pickup_datetime": lambda df: df.filter(F.col("pickup_datetime").isNull()).count(),
    "null_dropoff_datetime": lambda df: df.filter(F.col("dropoff_datetime").isNull()).count(),
    "invalid_trip_distance": lambda df: df.filter(F.col("trip_distance") <= 0).count(),
    "invalid_fare_amount": lambda df: df.filter(F.col("fare_amount") < 0).count(),
    "invalid_total_amount": lambda df: df.filter(F.col("total_amount") < 0).count(),
    "dropoff_before_pickup": lambda df: df.filter(F.col("dropoff_datetime") < F.col("pickup_datetime")).count(),
}


def run_checks(df):
    total = df.count()
    report = {"total_records": total}
    for name, check in CHECKS.items():
        count = check(df)
        report[name] = count
        report[f"{name}_rate"] = round(count / total, 4) if total else 0.0
    return report


def print_report(report: dict):
    print("Data Quality Report")
    print("-------------------")
    print(f"Total records: {report['total_records']:,}")
    for name in CHECKS:
        print(f"{name:28s} {report[name]:>10,}  ({report[f'{name}_rate']:.2%})")
