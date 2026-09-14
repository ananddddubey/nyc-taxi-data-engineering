# NYC Taxi Data Engineering Platform

PySpark + SQL medallion pipeline over NYC Yellow Taxi trip data, built as a
production-style Data Engineer portfolio project: Bronze -> Silver -> Gold,
a star-schema warehouse, data quality checks, incremental processing, and
(optionally) Docker, Airflow and AWS.

## Dataset

Source: [Kaggle - dhruvildave/new-york-city-taxi-trips-2019](https://www.kaggle.com/datasets/dhruvildave/new-york-city-taxi-trips-2019)
Field definitions: NYC TLC Yellow Taxi data dictionary (included in `docs/` if you
want to keep a copy — column names/semantics match what's coded in `config.yaml`).

You'll also need the TLC taxi zone lookup CSV (`taxi_zone_lookup.csv`, from the
same TLC trip-record-data page) — drop it at `data/taxi_zone_lookup.csv`.

> **Note on this repo's column mapping:** the official TLC dictionary this
> project was scoped against uses `tpep_pickup_datetime`, `PULocationID`, etc.
> Kaggle re-uploads sometimes vary the casing/naming release to release.
> `config.yaml -> processing.column_aliases` normalizes whatever the CSV uses
> to a consistent snake_case schema. **Before running Bronze on real data,
> open one raw CSV and confirm the aliases match** — add any that don't.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 1. Download the data (run locally — not in a network-sandboxed environment)

```bash
python -m src.ingestion.ingest
```

This pulls the Kaggle dataset via `kagglehub` (needs `kaggle.json` credentials
configured — see kagglehub docs) and stages the CSVs into `data/raw/`.

### 2. Run the pipeline

```bash
python -m src.transformation.bronze   # raw CSV -> partitioned Parquet
python -m src.transformation.silver   # clean, dedupe, derive fields, join zones
python -m src.transformation.gold     # star schema: dim_date, dim_location, dim_payment, dim_vendor, fact_trips
```

Re-running `bronze.py` is safe — files already recorded `SUCCESS` in
`data/audit/etl_audit.parquet` are skipped (see Incremental processing below).

### 3. Load into Postgres (optional, for the SQL layer / Power BI)

```bash
docker compose up -d postgres   # applies sql/schema.sql automatically
# then load gold/*.parquet into Postgres, e.g. via pandas.to_sql or psql \copy
```

Query examples: `sql/analytical_queries.sql`. Quality checks: `sql/data_quality.sql`.

### 4. Tests

```bash
pytest tests/
```

## Architecture

```
Kaggle CSV -> Bronze (raw Parquet, partitioned by year/month)
           -> Silver (cleaned, deduped, zone-enriched, rejects -> silver/rejected)
           -> Gold (star schema: fact_trips + dim_date/location/payment/vendor)
           -> Postgres -> Power BI / SQL queries
```

## Design decisions worth mentioning in an interview

- **Medallion architecture** — bronze preserves source fidelity; silver is
  where correctness lives; gold is shaped for consumption.
- **Broadcast join** for the taxi-zone dimension (small, static — avoids a
  shuffle against the much larger trips table).
- **Incremental processing** via `etl_audit` — reprocessing a month is a
  no-op unless you force it.
- **Rejects, not deletes** — invalid records are quarantined
  (`silver/rejected`) rather than silently dropped, so the reject rate is
  auditable.
- **Fail-fast** — if Bronze raises, the exception propagates and Silver/Gold
  never run on a half-loaded batch (see the `try/except` + re-raise in
  `bronze.py`; the Airflow DAG encodes the same dependency).

## What's stubbed vs. runnable as-is

- `src/ingestion/ingest.py`, `src/transformation/{bronze,silver,gold}.py`,
  `src/quality/validation.py`, and the SQL files are complete and meant to
  run against your actual downloaded data.
- `docker-compose.yml` / `Dockerfile` give you Postgres + a container to run
  the pipeline in; adjust the Spark image/JDK version if you hit
  compatibility issues locally.
- `airflow/nyc_taxi_dag.py` is a starting skeleton — wire it up after the
  pipeline runs cleanly by hand.
- AWS (S3 bronze/silver/gold, Glue, Athena, RDS) isn't implemented — that's
  Stage 4. Get this working locally first.
