"""
Airflow DAG - wire this up once the local pipeline (bronze -> silver -> gold)
runs cleanly by hand. Each stage is a separate task so a Silver failure
correctly blocks Gold from running on bad data.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"retries": 1}

with DAG(
    dag_id="nyc_taxi_pipeline",
    schedule_interval="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    ingest = BashOperator(task_id="ingest", bash_command="python src/ingestion/ingest.py")
    bronze = BashOperator(task_id="bronze_ingestion", bash_command="python -m src.transformation.bronze")
    silver = BashOperator(task_id="silver_transformation", bash_command="python -m src.transformation.silver")
    gold = BashOperator(task_id="gold_transformation", bash_command="python -m src.transformation.gold")

    ingest >> bronze >> silver >> gold
