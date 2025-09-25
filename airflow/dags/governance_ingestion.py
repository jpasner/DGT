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
    dag_id="governance_metadata_ingestion",
    description="Load curated governance views into OpenMetadata",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["openmetadata", "governance"],
) as dag:
    ingest_views = BashOperator(
        task_id="ingest_openmetadata_views",
        bash_command=(
            "OPENMETADATA_SERVER_AUTH_PROVIDER=no_auth "
            "metadata ingest -c /opt/airflow/openmetadata/ingestion.yaml"
        ),
    )

    profile_views = BashOperator(
        task_id="profile_openmetadata_views",
        bash_command=(
            "OPENMETADATA_SERVER_AUTH_PROVIDER=no_auth "
            "metadata profile -c /opt/airflow/openmetadata/profiler.yaml"
        ),
    )

    ingest_views >> profile_views
