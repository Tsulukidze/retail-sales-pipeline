"""Load parsed rows into the staging table."""

from __future__ import annotations

import csv
import io
import logging
from collections.abc import Sequence
from typing import Any

from retail_pipeline.db import DbConnection, transaction
from retail_pipeline.exceptions import StagingLoadError
from retail_pipeline.parsing import STAGING_ROW_COLUMNS, StagingRow

logger = logging.getLogger(__name__)

STAGING_TABLE = "staging.stg_retail_transactions"
COPY_COLUMNS: tuple[str, ...] = (*STAGING_ROW_COLUMNS, "batch_id", "source_file")


def load_staging_rows(
    connection: DbConnection,
    rows: Sequence[StagingRow],
    batch_id: str,
    source_file: str,
) -> int:
    """Replace everything in the staging table with `rows`.

    TRUNCATE and COPY run in one transaction. If something fails, the
    transaction is rolled back, and the table keeps its old content. So a
    failed load never leaves a half-filled table.

    Returns the number of rows in the table after the load.
    """
    buffer = _rows_to_csv(rows, batch_id, source_file)
    copy_sql = f"COPY {STAGING_TABLE} ({', '.join(COPY_COLUMNS)}) FROM STDIN WITH (FORMAT csv)"

    with transaction(connection), connection.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {STAGING_TABLE}")
        _copy_from_buffer(cursor, copy_sql, buffer)

        # Check that the table has exactly the rows I sent.
        cursor.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
        loaded = int(cursor.fetchone()[0])
        if loaded != len(rows):
            raise StagingLoadError(f"Sent {len(rows)} rows, but the table has {loaded}")

    logger.info("Loaded %d rows into %s (batch %s)", loaded, STAGING_TABLE, batch_id)
    return loaded


def _copy_from_buffer(cursor: Any, copy_sql: str, buffer: io.StringIO) -> None:
    """Run COPY ... FROM STDIN with the data in `buffer`.

    The two drivers have different methods for COPY:
    psycopg 3 has cursor.copy(), psycopg2 has cursor.copy_expert().
    """
    if hasattr(cursor, "copy"):  # psycopg 3
        with cursor.copy(copy_sql) as copy:
            copy.write(buffer.getvalue())
    else:  # psycopg2
        cursor.copy_expert(copy_sql, buffer)


def _rows_to_csv(rows: Sequence[StagingRow], batch_id: str, source_file: str) -> io.StringIO:
    """Write the rows as CSV text in memory, in the format COPY expects.

    The csv module puts values with line breaks (like store_location) in
    quotes, and COPY in CSV format reads them back correctly.
    For 100,000 rows this needs only a few MB of memory. For much bigger
    files, I would write to a temporary file instead.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for row in rows:
        writer.writerow((*row.as_tuple(), batch_id, source_file))
    buffer.seek(0)
    return buffer
