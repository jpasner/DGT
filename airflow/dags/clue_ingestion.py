"""Orchestrates OpenMetadata ingestion from PostgreSQL via Airflow."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "governance-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="clue_metadata_ingestion",
    description="Load curated clue views into OpenMetadata",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["openmetadata", "governance"],
) as dag:
    ingest_views = BashOperator(
        task_id="ingest_openmetadata_views",
        bash_command=(
            "metadata ingest -c /opt/airflow/openmetadata/clue_ingestion.yaml"
        ),
    )

    profile_views = BashOperator(
        task_id="profile_openmetadata_views",
        bash_command=(
            "metadata profile -c /opt/airflow/openmetadata/clue_profiler.yaml"
        ),
    )

    ingest_lineage = BashOperator(
        task_id="ingest_openmetadata_lineage",
        bash_command=(
            "metadata ingest -c /opt/airflow/openmetadata/clue_lineage.yaml"
        ),
    )

    ingest_views >> profile_views >> ingest_lineage
