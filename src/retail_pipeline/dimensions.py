"""Load the store and product dimensions.

These two tables need random names, and the names must be repeatable. That
is easier in Python than in SQL, so they are loaded from here. The other
dimensions only need data from staging, so they are plain SQL files in
sql/transform/.

Both loads only add new codes. Existing rows are never changed, so a store
keeps its ID and its name forever.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from retail_pipeline.db import DbConnection, transaction
from retail_pipeline.names import NameGenerator

logger = logging.getLogger(__name__)

SELECT_STORE_CODES = """
    SELECT DISTINCT store_code
    FROM staging.stg_retail_transactions
    ORDER BY store_code
"""

SELECT_PRODUCT_CODES = """
    SELECT DISTINCT product_code
    FROM staging.stg_retail_transactions
    ORDER BY product_code
"""

# unnest() turns the two lists (codes and names) into rows.
# ORDER BY gives IDs in code order on a new database (A = 1, B = 2, ...).
# WHERE NOT EXISTS sends only new codes to the INSERT. Without it, every run
# would use up ID numbers for codes that already exist, and the IDs would
# get gaps. ON CONFLICT is an extra safety net if two runs overlap.
INSERT_NEW_STORES = """
    INSERT INTO core.dim_store (store_code, store_name)
    SELECT new_rows.code, new_rows.name
    FROM unnest(%s::varchar[], %s::varchar[]) AS new_rows (code, name)
    WHERE NOT EXISTS (
        SELECT 1 FROM core.dim_store AS existing WHERE existing.store_code = new_rows.code
    )
    ORDER BY new_rows.code
    ON CONFLICT (store_code) DO NOTHING
    RETURNING store_code
"""

INSERT_NEW_PRODUCTS = """
    INSERT INTO core.dim_product (product_code, product_name)
    SELECT new_rows.code, new_rows.name
    FROM unnest(%s::varchar[], %s::varchar[]) AS new_rows (code, name)
    WHERE NOT EXISTS (
        SELECT 1 FROM core.dim_product AS existing WHERE existing.product_code = new_rows.code
    )
    ORDER BY new_rows.code
    ON CONFLICT (product_code) DO NOTHING
    RETURNING product_code
"""


def load_store_dimension(connection: DbConnection, generator: NameGenerator) -> int:
    """Add stores that are in staging but not yet in dim_store. Returns how many were added."""
    return _load_named_dimension(
        connection, SELECT_STORE_CODES, INSERT_NEW_STORES, generator.store_name, "dim_store"
    )


def load_product_dimension(connection: DbConnection, generator: NameGenerator) -> int:
    """Add products that are in staging but not yet in dim_product. Returns how many were added."""
    return _load_named_dimension(
        connection, SELECT_PRODUCT_CODES, INSERT_NEW_PRODUCTS, generator.product_name, "dim_product"
    )


def _load_named_dimension(
    connection: DbConnection,
    select_codes_sql: str,
    insert_sql: str,
    make_name: Callable[[str], str],
    table_label: str,
) -> int:
    with transaction(connection), connection.cursor() as cursor:
        cursor.execute(select_codes_sql)
        codes = [row[0] for row in cursor.fetchall()]

        # Names are made for all codes, but the INSERT keeps only new ones.
        # With a few dozen codes, this costs nothing.
        names = [make_name(code) for code in codes]

        cursor.execute(insert_sql, (codes, names))
        added = len(cursor.fetchall())

    logger.info("%s: %d codes in staging, %d new rows added", table_label, len(codes), added)
    return added
