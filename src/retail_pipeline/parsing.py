"""Turn rows from the source CSV into typed rows for the staging table.

All functions here are pure: they do not use the database or the network.
This makes them easy to test.
"""

from __future__ import annotations

import csv
import hashlib
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from retail_pipeline.exceptions import RowParsingError, SourceValidationError
from retail_pipeline.source_schema import EXPECTED_SOURCE_COLUMNS, SOURCE_DATE_FORMAT

logger = logging.getLogger(__name__)

# The last line of an address looks like "Baileyfort, HI 93354" or "FPO AE 12345".
# The state code is the two capital letters before the ZIP code.
STATE_CODE_PATTERN = re.compile(r"\b([A-Z]{2})\s+\d{5}(?:-\d{4})?\s*$")

# Character used to join values before hashing. It never appears in the data,
# so different rows cannot produce the same joined text.
HASH_SEPARATOR = "\x1f"

# How many problems to show in the error message when rows cannot be parsed.
MAX_ERRORS_IN_MESSAGE = 5


@dataclass(frozen=True, slots=True)
class StagingRow:
    """One source row with correct data types.

    The field order is the same as the column order in the staging table.
    """

    customer_id: int
    product_code: str
    quantity: int
    unit_price: Decimal
    transaction_ts: datetime
    payment_method: str
    store_location: str
    store_code: str
    product_category: str
    discount_pct: Decimal
    total_amount: Decimal
    source_row_hash: str

    def as_tuple(self) -> tuple[object, ...]:
        """Return the values in column order."""
        return tuple(getattr(self, name) for name in STAGING_ROW_COLUMNS)


STAGING_ROW_COLUMNS: tuple[str, ...] = tuple(field.name for field in fields(StagingRow))


def derive_store_code(store_location: str) -> str:
    r"""Return the state code from an address, e.g. 'HI'.

    Example: '176 Andrew Cliffs\nBaileyfort, HI 93354' -> 'HI'.
    I use the state code as the store, because the source has no store ID.
    """
    lines = store_location.strip().splitlines()
    match = STATE_CODE_PATTERN.search(lines[-1]) if lines else None
    if match is None:
        raise ValueError("no state code found in the address")
    return match.group(1)


def compute_row_hash(raw_row: Mapping[str, str | None]) -> str:
    """Return the md5 hash of the original values of a source row.

    I use the raw text, before any type conversion, so the hash describes
    the row exactly as it is in the file. Only the expected columns are used,
    so a new extra column in the file does not change the hash.
    """
    text = HASH_SEPARATOR.join(raw_row.get(column) or "" for column in EXPECTED_SOURCE_COLUMNS)
    # md5 is used only to identify rows, not for security.
    return hashlib.md5(text.encode("utf-8"), usedforsecurity=False).hexdigest()


def parse_source_row(raw_row: Mapping[str, str | None]) -> StagingRow:
    """Convert one raw CSV row (all values are text) into a StagingRow.

    Raises RowParsingError if a value is missing or has the wrong format.
    """
    store_location = _text(raw_row, "StoreLocation")
    return StagingRow(
        customer_id=_integer(raw_row, "CustomerID"),
        product_code=_text(raw_row, "ProductID"),
        quantity=_integer(raw_row, "Quantity"),
        unit_price=_decimal(raw_row, "Price"),
        transaction_ts=_timestamp(raw_row, "TransactionDate"),
        payment_method=_text(raw_row, "PaymentMethod"),
        store_location=store_location,
        store_code=_store_code(store_location),
        product_category=_text(raw_row, "ProductCategory"),
        discount_pct=_decimal(raw_row, "DiscountApplied(%)"),
        total_amount=_decimal(raw_row, "TotalAmount"),
        source_row_hash=compute_row_hash(raw_row),
    )


def read_staging_rows(csv_path: Path) -> list[StagingRow]:
    """Read and parse all rows of the source file.

    I check every row first and collect all problems. If any row is wrong,
    nothing is loaded: a partly loaded table would give wrong totals
    without anyone noticing.
    """
    rows: list[StagingRow] = []
    errors: list[str] = []

    with csv_path.open(newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        reader.fieldnames = [name.strip() for name in reader.fieldnames or []]

        for row_number, raw_row in enumerate(reader, start=1):
            try:
                rows.append(parse_source_row(raw_row))
            except RowParsingError as exc:
                errors.append(f"row {row_number}: {exc}")

    if errors:
        examples = "; ".join(errors[:MAX_ERRORS_IN_MESSAGE])
        raise SourceValidationError(
            f"{len(errors)} rows in {csv_path.name} could not be parsed. "
            f"First problems: {examples}"
        )

    logger.info("Parsed %d rows from %s", len(rows), csv_path.name)
    return rows


# --- Helpers for single values ---------------------------------------------


def _text(raw_row: Mapping[str, str | None], column: str) -> str:
    value = (raw_row.get(column) or "").strip()
    if not value:
        raise RowParsingError(column, raw_row.get(column), "value is empty")
    return value


def _integer(raw_row: Mapping[str, str | None], column: str) -> int:
    value = _text(raw_row, column)
    try:
        return int(value)
    except ValueError:
        raise RowParsingError(column, value, "not a whole number") from None


def _decimal(raw_row: Mapping[str, str | None], column: str) -> Decimal:
    # Decimal keeps the exact value from the file. A float could change
    # 80.08 into 80.0799999..., which matters for money.
    value = _text(raw_row, column)
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise RowParsingError(column, value, "not a number") from None
    if not number.is_finite():
        raise RowParsingError(column, value, "not a finite number")
    return number


def _timestamp(raw_row: Mapping[str, str | None], column: str) -> datetime:
    value = _text(raw_row, column)
    try:
        return datetime.strptime(value, SOURCE_DATE_FORMAT)
    except ValueError:
        raise RowParsingError(column, value, f"expected format {SOURCE_DATE_FORMAT}") from None


def _store_code(store_location: str) -> str:
    try:
        return derive_store_code(store_location)
    except ValueError as exc:
        raise RowParsingError("StoreLocation", store_location, str(exc)) from None
