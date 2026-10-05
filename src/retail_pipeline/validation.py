"""Check the structure of the downloaded file.

This runs before anything is written to the database. If the file is broken or
its columns have changed, the pipeline stops here with a clear message.
Checks of single values (types, dates) happen later, in the staging load.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from retail_pipeline.exceptions import SourceValidationError
from retail_pipeline.source_schema import EXPECTED_SOURCE_COLUMNS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileValidationReport:
    """Result of a successful file check."""

    path: Path
    row_count: int
    columns: tuple[str, ...]


def validate_source_file(
    path: Path,
    expected_columns: Sequence[str] = EXPECTED_SOURCE_COLUMNS,
    min_rows: int = 1,
) -> FileValidationReport:
    """Check that the file exists, has all expected columns and enough rows.

    A missing column stops the pipeline. An extra column only writes a
    warning: the pipeline can still work, but someone should look at it.
    """
    if not path.is_file():
        raise SourceValidationError(f"Source file not found: {path}")

    # "utf-8-sig" also works for files that start with a byte order mark (BOM).
    with path.open(newline="", encoding="utf-8-sig") as file:
        reader = csv.reader(file)
        header = next(reader, None)
        if header is None:
            raise SourceValidationError(f"Source file is empty: {path}")

        columns = tuple(name.strip() for name in header)
        missing = [name for name in expected_columns if name not in columns]
        if missing:
            raise SourceValidationError(f"Missing columns in {path.name}: {missing}")

        extra = [name for name in columns if name not in expected_columns]
        if extra:
            logger.warning("Unexpected columns in %s will be ignored: %s", path.name, extra)

        # StoreLocation values contain line breaks inside quotes. csv.reader
        # understands this, so it counts real rows, not lines of text.
        row_count = sum(1 for _ in reader)

    if row_count < min_rows:
        raise SourceValidationError(
            f"Source file {path.name} has {row_count} rows, expected at least {min_rows}"
        )

    logger.info("Validated %s: %d rows, %d columns", path.name, row_count, len(columns))
    return FileValidationReport(path=path, row_count=row_count, columns=columns)
