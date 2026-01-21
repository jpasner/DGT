"""Airflow DAG to ingest base governance tables into OpenMetadata."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "governance-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="governance_base_table_ingestion",
    description="Ingest governance base tables from Postgres into OpenMetadata",
    default_args=DEFAULT_ARGS,
    schedule="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["openmetadata", "governance", "tables"],
) as dag:
    ingest_base_tables = BashOperator(
        task_id="ingest_base_tables",
        bash_command="metadata ingest -c /opt/airflow/openmetadata/base_ingestion.yaml",
    )

    profile_base_tables = BashOperator(
        task_id="profile_base_tables",
        bash_command="metadata profile -c /opt/airflow/openmetadata/profiler.yaml",
    )

    ingest_base_tables >> profile_base_tables
