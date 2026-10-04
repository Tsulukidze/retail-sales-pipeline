"""Daily retail sales pipeline: Kaggle -> staging -> core -> mart.

This file only defines the tasks and their order. The real work is done by
the `retail_pipeline` package (Python) and the files in `sql/` (SQL).

Tasks made with EmptyOperator are placeholders. I will replace them step by step.
"""

from __future__ import annotations

from contextlib import closing
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup, dag, get_current_context, task

from retail_pipeline.config import LOCAL_TIMEZONE, PipelineConfig
from retail_pipeline.extract import KaggleDatasetDownloader, prune_old_snapshots, snapshot_name
from retail_pipeline.parsing import read_staging_rows
from retail_pipeline.staging_loader import load_staging_rows
from retail_pipeline.validation import validate_source_file

CONFIG = PipelineConfig.from_env()

# SQL files that create the tables. They run in file name order (001, 010, ...).
DDL_FILES = sorted(
    str(path.relative_to(CONFIG.sql_dir)) for path in CONFIG.sql_dir.glob("ddl/*.sql")
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="retail_sales_pipeline",
    description="Daily retail sales pipeline: Kaggle -> PostgreSQL DWH",
    schedule="0 0 * * *",  # every day at 00:00 Tbilisi time
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    template_searchpath=[str(CONFIG.sql_dir)],
    tags=["retail", "elt"],
)
def retail_sales_pipeline():
    @task
    def check_dwh_connection() -> str:
        """Stop early if the warehouse is not reachable."""
        hook = PostgresHook(postgres_conn_id=CONFIG.dwh_conn_id)
        version = hook.get_first("SELECT version();")[0]
        print(f"Connected to DWH: {version}")
        return version

    init_schema = SQLExecuteQueryOperator(
        task_id="init_schema",
        conn_id=CONFIG.dwh_conn_id,
        sql=DDL_FILES,
    )

    # Only this task has retries. A network or Kaggle problem is often
    # temporary, so trying again can help. In the other tasks, an error usually
    # means a bug or bad data, and trying again would not help.
    @task(retries=3, retry_delay=timedelta(minutes=2), retry_exponential_backoff=True)
    def extract_dataset() -> str:
        # We use run_after (the time the run started) for the folder name.
        # In Airflow 3, a run started by hand may have no logical date, so we
        # cannot use `ds` here.
        run_after = get_current_context()["dag_run"].run_after
        target_dir = CONFIG.raw_dir / snapshot_name(run_after, LOCAL_TIMEZONE)

        csv_path = KaggleDatasetDownloader(CONFIG.kaggle_dataset).download(target_dir)
        prune_old_snapshots(CONFIG.raw_dir, keep=CONFIG.raw_snapshots_to_keep)
        return str(csv_path)

    @task
    def validate_source(csv_path: str) -> str:
        report = validate_source_file(Path(csv_path))
        print(f"Source file OK: {report.row_count:,} rows")
        return csv_path

    @task
    def load_staging(csv_path: str) -> int:
        rows = read_staging_rows(Path(csv_path))

        # The Airflow run ID shows which run loaded each row.
        batch_id = get_current_context()["run_id"]

        hook = PostgresHook(postgres_conn_id=CONFIG.dwh_conn_id)
        with closing(hook.get_conn()) as connection:
            loaded = load_staging_rows(connection, rows, batch_id=batch_id, source_file=csv_path)

        print(f"Loaded {loaded:,} rows into staging")
        return loaded

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

    csv_path = extract_dataset()
    validated_csv_path = validate_source(csv_path)
    staged_row_count = load_staging(validated_csv_path)

    check_dwh_connection() >> init_schema >> csv_path
    (
        staged_row_count
        >> load_dimensions
        >> load_fact_sales
        >> build_agg_sales_daily
        >> [build_store_lfl, update_product_abc]
        >> run_data_quality_checks
    )


retail_sales_pipeline()
