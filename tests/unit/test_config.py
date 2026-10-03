from pathlib import Path

from retail_pipeline.config import DEFAULT_KAGGLE_DATASET, DWH_CONN_ID, PipelineConfig


def test_defaults_are_used_when_env_is_empty():
    config = PipelineConfig.from_env(env={})

    assert config.kaggle_dataset == DEFAULT_KAGGLE_DATASET
    assert config.data_dir == Path("data")
    assert config.sql_dir == Path("sql")
    assert config.dwh_conn_id == DWH_CONN_ID


def test_values_are_read_from_env():
    env = {
        "PIPELINE_KAGGLE_DATASET": "owner/other-dataset",
        "PIPELINE_DATA_DIR": "/opt/airflow/data",
        "PIPELINE_SQL_DIR": "/opt/airflow/sql",
    }

    config = PipelineConfig.from_env(env=env)

    assert config.kaggle_dataset == "owner/other-dataset"
    assert config.raw_dir == Path("/opt/airflow/data/raw")
    assert config.sql_dir == Path("/opt/airflow/sql")
