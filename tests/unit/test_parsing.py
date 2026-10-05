import csv
from datetime import datetime
from decimal import Decimal

import pytest

from retail_pipeline.exceptions import RowParsingError, SourceValidationError
from retail_pipeline.parsing import (
    STAGING_ROW_COLUMNS,
    compute_row_hash,
    derive_store_code,
    parse_source_row,
    read_staging_rows,
)
from retail_pipeline.source_schema import EXPECTED_SOURCE_COLUMNS


def make_raw_row(**changes: str) -> dict[str, str]:
    """A valid source row. Pass column=value to change single values."""
    row = {
        "CustomerID": "109318",
        "ProductID": "C",
        "Quantity": "7",
        "Price": "80.08",
        "TransactionDate": "12/26/2023 12:32",
        "PaymentMethod": "Cash",
        "StoreLocation": "176 Andrew Cliffs\nBaileyfort, HI 93354",
        "ProductCategory": "Books",
        "DiscountApplied(%)": "18.68",
        "TotalAmount": "455.86",
    }
    row.update(changes)
    return row


def write_csv(path, rows: list[dict[str, str]]):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=EXPECTED_SOURCE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


# --- derive_store_code -----------------------------------------------------


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        ("176 Andrew Cliffs\nBaileyfort, HI 93354", "HI"),
        ("910 Mendez Ville Suite 909\nPort Lauraland, MO 99563", "MO"),
        ("USNS Smith\nFPO AE 12345", "AE"),  # military address
        ("Unit 1234 Box 5678\nDPO AP 12345-6789", "AP"),  # ZIP+4
    ],
)
def test_derive_store_code(address, expected):
    assert derive_store_code(address) == expected


@pytest.mark.parametrize("address", ["", "No state here", "Somewhere, hi 93354"])
def test_derive_store_code_fails_without_state(address):
    with pytest.raises(ValueError, match="no state code"):
        derive_store_code(address)


# --- compute_row_hash ------------------------------------------------------


def test_row_hash_is_stable_and_md5_sized():
    first = compute_row_hash(make_raw_row())
    second = compute_row_hash(make_raw_row())

    assert first == second
    assert len(first) == 32


def test_row_hash_changes_when_a_value_changes():
    assert compute_row_hash(make_raw_row()) != compute_row_hash(make_raw_row(Quantity="8"))


def test_row_hash_ignores_extra_columns():
    with_extra = {**make_raw_row(), "NewColumn": "x"}

    assert compute_row_hash(with_extra) == compute_row_hash(make_raw_row())


# --- parse_source_row ------------------------------------------------------


def test_parse_source_row_converts_types():
    row = parse_source_row(make_raw_row())

    assert row.customer_id == 109318
    assert row.product_code == "C"
    assert row.quantity == 7
    assert row.unit_price == Decimal("80.08")
    assert row.transaction_ts == datetime(2023, 12, 26, 12, 32)
    assert row.store_location == "176 Andrew Cliffs\nBaileyfort, HI 93354"
    assert row.store_code == "HI"
    assert row.discount_pct == Decimal("18.68")
    assert row.total_amount == Decimal("455.86")


def test_as_tuple_follows_column_order():
    row = parse_source_row(make_raw_row())

    values = dict(zip(STAGING_ROW_COLUMNS, row.as_tuple(), strict=True))

    assert values["store_code"] == "HI"
    assert values["source_row_hash"] == row.source_row_hash


@pytest.mark.parametrize(
    ("column", "bad_value"),
    [
        ("CustomerID", "abc"),
        ("Quantity", "7.5"),
        ("Price", "eighty"),
        ("Price", "NaN"),
        ("TransactionDate", "2023-12-26 12:32"),  # wrong date format
        ("PaymentMethod", "   "),
        ("StoreLocation", "no state code"),
    ],
)
def test_parse_source_row_rejects_bad_values(column, bad_value):
    with pytest.raises(RowParsingError) as error:
        parse_source_row(make_raw_row(**{column: bad_value}))

    assert error.value.column == column


# --- read_staging_rows -----------------------------------------------------


def test_read_staging_rows_reads_all_rows(tmp_path):
    path = write_csv(tmp_path / "data.csv", [make_raw_row(), make_raw_row(CustomerID="2")])

    rows = read_staging_rows(path)

    assert [row.customer_id for row in rows] == [109318, 2]


def test_read_staging_rows_reports_all_bad_rows(tmp_path):
    rows = [make_raw_row(), make_raw_row(Quantity="x"), make_raw_row(Price="y")]
    path = write_csv(tmp_path / "data.csv", rows)

    with pytest.raises(SourceValidationError) as error:
        read_staging_rows(path)

    message = str(error.value)
    assert message.startswith("2 rows")
    assert "row 2: Quantity" in message
    assert "row 3: Price" in message
