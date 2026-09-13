"""
Stage 1 - Bronze layer.

Reads raw monthly CSVs, does the absolute minimum (schema normalization +
year/month partition columns), and writes Parquet. No filtering, no
business logic - bronze preserves the source as faithfully as possible.

Also implements incremental processing: a file already recorded as
'SUCCESS' in the audit log is skipped on rerun.
"""
import os
import re
from datetime import datetime

from pyspark.sql import functions as F

from src.utils.spark_utils import load_config, get_spark, normalize_columns

FILENAME_DATE_RE = re.compile(r"(20\d{2})[-_]?(\d{2})")


def already_processed(spark, audit_log_path: str, file_name: str) -> bool:
    if not os.path.exists(audit_log_path):
        return False
    audit = spark.read.parquet(audit_log_path)
    hit = audit.filter(
        (F.col("file_name") == file_name) & (F.col("status") == "SUCCESS")
    ).limit(1).count()
    return hit > 0


def append_audit_record(spark, audit_log_path: str, record: dict):
    row = spark.createDataFrame([record])
    if os.path.exists(audit_log_path):
        row.write.mode("append").parquet(audit_log_path)
    else:
        row.write.mode("overwrite").parquet(audit_log_path)


def process_file(spark, cfg, file_path: str):
    file_name = os.path.basename(file_path)
    start_time = datetime.utcnow()

    if already_processed(spark, cfg["paths"]["audit_log"], file_name):
        print(f"SKIP (already processed): {file_name}")
        return

    match = FILENAME_DATE_RE.search(file_name)
    if not match:
        raise ValueError(f"Could not infer year/month from filename: {file_name}")
    year, month = match.group(1), match.group(2)

    print(f"Reading {file_name} ...")
    df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
    df = normalize_columns(df, cfg["processing"]["column_aliases"])

    record_count = df.count()

    df = (
        df.withColumn("source_file", F.lit(file_name))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("year", F.lit(int(year)))
        .withColumn("month", F.lit(int(month)))
    )

    out_path = cfg["paths"]["bronze_dir"]
    df.write.mode("append").partitionBy("year", "month").parquet(out_path)

    append_audit_record(
        spark,
        cfg["paths"]["audit_log"],
        {
            "batch_id": f"{year}-{month}",
            "file_name": file_name,
            "processing_date": datetime.utcnow().isoformat(),
            "record_count": record_count,
            "status": "SUCCESS",
            "start_time": start_time.isoformat(),
            "end_time": datetime.utcnow().isoformat(),
            "error_message": "",
        },
    )
    print(f"DONE {file_name}: {record_count:,} records -> {out_path} (year={year}, month={month})")


def run():
    cfg = load_config()
    spark = get_spark(cfg)

    raw_dir = cfg["paths"]["raw_dir"]
    files = sorted(
        os.path.join(raw_dir, f) for f in os.listdir(raw_dir) if f.lower().endswith(".csv")
    )
    if not files:
        raise FileNotFoundError(f"No CSV files found in {raw_dir}. Run ingest.py first.")

    for f in files:
        try:
            process_file(spark, cfg, f)
        except Exception as e:
            print(f"FAILED {os.path.basename(f)}: {e}")
            append_audit_record(
                spark,
                cfg["paths"]["audit_log"],
                {
                    "batch_id": os.path.basename(f),
                    "file_name": os.path.basename(f),
                    "processing_date": datetime.utcnow().isoformat(),
                    "record_count": 0,
                    "status": "FAILED",
                    "start_time": datetime.utcnow().isoformat(),
                    "end_time": datetime.utcnow().isoformat(),
                    "error_message": str(e),
                },
            )
            raise  # stop the pipeline - silver should never run on a failed bronze batch

    spark.stop()


if __name__ == "__main__":
    run()
