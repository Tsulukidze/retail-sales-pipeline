"""Download the source dataset from Kaggle into a folder for each day."""

from __future__ import annotations

import logging
import shutil
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from retail_pipeline.exceptions import SourceExtractionError

logger = logging.getLogger(__name__)


class KaggleClient(Protocol):
    """The Kaggle API methods that this module needs."""

    def authenticate(self) -> None: ...

    def dataset_download_files(
        self,
        dataset: str,
        path: str | None = None,
        force: bool = False,
        quiet: bool = True,
        unzip: bool = False,
    ) -> None: ...


def default_client_factory() -> KaggleClient:
    """Create the real Kaggle client.

    We import `kaggle` here, not at the top of the file. The `kaggle` package
    tries to log in as soon as it is imported. At the top of the file, this
    would break DAG loading and unit tests on machines without credentials.
    """
    from kaggle.api.kaggle_api_extended import KaggleApi

    return KaggleApi()


def snapshot_name(moment: datetime, timezone: str) -> str:
    """Return the folder name for a download, e.g. '2026-10-04'.

    `moment` must include a timezone. We convert it to the local timezone
    first, so a run just after midnight in Tbilisi gets the Tbilisi date.
    """
    if moment.tzinfo is None:
        raise ValueError("moment must include a timezone")
    return moment.astimezone(ZoneInfo(timezone)).date().isoformat()


class KaggleDatasetDownloader:
    """Downloads a Kaggle dataset and returns the path of its CSV file."""

    def __init__(
        self,
        dataset: str,
        client_factory: Callable[[], KaggleClient] = default_client_factory,
    ) -> None:
        self._dataset = dataset
        self._client_factory = client_factory

    def download(self, target_dir: Path) -> Path:
        """Download the dataset into `target_dir`.

        The folder is deleted and created again first. So if the same run
        happens twice, the result is the same, and no old files stay behind.
        """
        if target_dir.exists():
            shutil.rmtree(target_dir)
        target_dir.mkdir(parents=True)

        logger.info("Downloading Kaggle dataset %s into %s", self._dataset, target_dir)
        client = self._client_factory()
        try:
            client.authenticate()
            client.dataset_download_files(
                self._dataset, path=str(target_dir), force=True, quiet=True, unzip=True
            )
        except Exception as exc:  # any error from the external Kaggle library
            raise SourceExtractionError(
                f"Failed to download Kaggle dataset '{self._dataset}': {exc}"
            ) from exc

        csv_path = self._find_single_csv(target_dir)
        logger.info("Downloaded %s (%.1f MB)", csv_path.name, csv_path.stat().st_size / 1e6)
        return csv_path

    @staticmethod
    def _find_single_csv(directory: Path) -> Path:
        csv_files = sorted(directory.glob("*.csv"))
        if len(csv_files) != 1:
            names = [path.name for path in csv_files]
            raise SourceExtractionError(
                f"Expected exactly one CSV file in {directory}, found {len(csv_files)}: {names}"
            )
        return csv_files[0]


def prune_old_snapshots(raw_dir: Path, keep: int) -> list[Path]:
    """Delete old download folders and keep only the newest `keep` folders.

    Folder names are dates (YYYY-MM-DD), so sorting them by name also sorts
    them by date. Returns the deleted folders.
    """
    if keep < 1:
        raise ValueError("keep must be at least 1")
    if not raw_dir.exists():
        return []

    snapshots = sorted(path for path in raw_dir.iterdir() if path.is_dir())
    to_delete = snapshots[:-keep]
    for path in to_delete:
        shutil.rmtree(path)
        logger.info("Deleted old snapshot %s", path)
    return to_delete
