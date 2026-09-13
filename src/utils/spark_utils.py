"""Shared Spark session + config helpers."""
import yaml
from pyspark.sql import SparkSession


def load_config(path: str = "config/config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_spark(cfg: dict) -> SparkSession:
    spark_cfg = cfg.get("spark", {})
    return (
        SparkSession.builder
        .appName(spark_cfg.get("app_name", "nyc-taxi-pipeline"))
        .config("spark.sql.shuffle.partitions", spark_cfg.get("shuffle_partitions", 8))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def normalize_columns(df, alias_map: dict):
    """Rename whatever casing the source CSV used to our canonical snake_case names."""
    lower_to_actual = {c.lower(): c for c in df.columns}
    for source_alias, canonical in alias_map.items():
        actual = lower_to_actual.get(source_alias.lower())
        if actual and actual != canonical:
            df = df.withColumnRenamed(actual, canonical)
    return df
