"""Small helpers for working with a database connection.

They work with both Postgres drivers: psycopg2 and psycopg 3. Airflow's
PostgresHook gives a psycopg 3 connection when psycopg 3 is installed, and a
psycopg2 connection otherwise.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Protocol


class DbConnection(Protocol):
    """The connection methods this project needs. Both drivers have them."""

    def cursor(self) -> Any:
        """Return a new cursor."""

    def commit(self) -> None:
        """Save the current transaction."""

    def rollback(self) -> None:
        """Undo the current transaction."""


@contextmanager
def transaction(connection: DbConnection) -> Iterator[None]:
    """Commit at the end of the block, or roll back if there is an error.

    Usage:
        with transaction(connection), connection.cursor() as cursor:
            cursor.execute(...)

    I do not use `with connection:` here, because the two drivers handle it
    differently: psycopg 3 also closes the connection at the end, psycopg2
    does not.
    """
    try:
        yield
        connection.commit()
    except Exception:
        connection.rollback()
        raise
