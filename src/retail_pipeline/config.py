"""Pipeline configuration, read once from environment variables.

All environment access is centralised here so the rest of the code receives
plain, typed values and stays easy to test.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

DEFAULT_KAGGLE_DATASET = "fahadrehman07/retail-transaction-dataset"
DWH_CONN_ID = "dwh_postgres"


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable settings shared by all pipeline steps."""

    kaggle_dataset: str
    data_dir: Path
    sql_dir: Path
    dwh_conn_id: str = DWH_CONN_ID

    @property
    def raw_dir(self) -> Path:
        """Directory where downloaded source files are stored."""
        return self.data_dir / "raw"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> PipelineConfig:
        """Build the config from environment variables (or a given mapping for tests)."""
        env = os.environ if env is None else env
        return cls(
            kaggle_dataset=env.get("PIPELINE_KAGGLE_DATASET", DEFAULT_KAGGLE_DATASET),
            data_dir=Path(env.get("PIPELINE_DATA_DIR", "data")),
            sql_dir=Path(env.get("PIPELINE_SQL_DIR", "sql")),
        )
