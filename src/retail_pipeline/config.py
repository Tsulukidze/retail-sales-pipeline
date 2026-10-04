"""Pipeline settings, read from environment variables.

Only this module reads environment variables. Other modules get normal,
typed values from here, which makes them easier to test.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

DEFAULT_KAGGLE_DATASET = "fahadrehman07/retail-transaction-dataset"
DWH_CONN_ID = "dwh_postgres"
LOCAL_TIMEZONE = "Asia/Tbilisi"


@dataclass(frozen=True)
class PipelineConfig:
    """Settings used by all pipeline steps. They cannot be changed after creation."""

    kaggle_dataset: str
    data_dir: Path
    sql_dir: Path
    dwh_conn_id: str = DWH_CONN_ID
    raw_snapshots_to_keep: int = 7

    @property
    def raw_dir(self) -> Path:
        """Folder where downloaded source files are saved."""
        return self.data_dir / "raw"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> PipelineConfig:
        """Create the settings from environment variables.

        Tests can pass their own `env` dictionary instead.
        """
        env = os.environ if env is None else env
        return cls(
            kaggle_dataset=env.get("PIPELINE_KAGGLE_DATASET", DEFAULT_KAGGLE_DATASET),
            data_dir=Path(env.get("PIPELINE_DATA_DIR", "data")),
            sql_dir=Path(env.get("PIPELINE_SQL_DIR", "sql")),
        )
