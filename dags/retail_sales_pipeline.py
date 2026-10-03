"""Daily retail sales ELT pipeline: Kaggle -> staging -> core -> mart.

The DAG file only wires tasks together. Business logic lives in the
`retail_pipeline` package (Python) and the `sql/` folder (SQL).

Skeleton stage: schedule, defaults and task structure are final. Tasks built
with EmptyOperator are placeholders that will be replaced one by one.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup, dag, task

from retail_pipeline.config import DWH_CONN_ID

LOCAL_TZ = pendulum.timezone("Asia/Tbilisi")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="retail_sales_pipeline",
    description="Daily retail sales ELT: Kaggle -> PostgreSQL DWH",
    schedule="0 0 * * *",  # every day at 00:00 Asia/Tbilisi
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["retail", "elt"],
)
def retail_sales_pipeline():
    @task
    def check_dwh_connection() -> str:
        """Fail fast if the warehouse is unreachable."""
        hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        version = hook.get_first("SELECT version();")[0]
        print(f"Connected to DWH: {version}")
        return version

    init_schema = EmptyOperator(task_id="init_schema")
    extract_dataset = EmptyOperator(task_id="extract_dataset")
    validate_source_file = EmptyOperator(task_id="validate_source_file")
    load_staging = EmptyOperator(task_id="load_staging")

    with TaskGroup(group_id="load_dimensions") as load_dimensions:
        EmptyOperator(task_id="load_dim_date")
        EmptyOperator(task_id="load_dim_store")
        EmptyOperator(task_id="load_dim_product")
        EmptyOperator(task_id="load_lookup_dimensions")

    load_fact_sales = EmptyOperator(task_id="load_fact_sales")
    build_agg_sales_daily = EmptyOperator(task_id="build_agg_sales_daily")
    build_store_lfl = EmptyOperator(task_id="build_store_lfl")
    update_product_abc = EmptyOperator(task_id="update_product_abc")
    run_data_quality_checks = EmptyOperator(task_id="run_data_quality_checks")

    (
        check_dwh_connection()
        >> init_schema
        >> extract_dataset
        >> validate_source_file
        >> load_staging
        >> load_dimensions
        >> load_fact_sales
        >> build_agg_sales_daily
        >> [build_store_lfl, update_product_abc]
        >> run_data_quality_checks
    )


retail_sales_pipeline()
