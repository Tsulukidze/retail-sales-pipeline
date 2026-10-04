import pytest

from retail_pipeline.exceptions import SourceValidationError
from retail_pipeline.source_schema import EXPECTED_SOURCE_COLUMNS
from retail_pipeline.validation import validate_source_file

HEADER = ",".join(EXPECTED_SOURCE_COLUMNS)
ROW = (
    "109318,C,7,80.08,12/26/2023 12:32,Cash,"
    '"176 Andrew Cliffs\nBaileyfort, HI 93354",Books,18.68,455.86'
)


def write(tmp_path, content: str, name: str = "data.csv"):
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


def test_valid_file_counts_records_not_lines(tmp_path):
    # Each row has a line break inside quotes, so the file has more lines than rows.
    path = write(tmp_path, f"{HEADER}\n{ROW}\n{ROW}\n")

    report = validate_source_file(path)

    assert report.row_count == 2
    assert report.columns == EXPECTED_SOURCE_COLUMNS


def test_missing_file_fails(tmp_path):
    with pytest.raises(SourceValidationError, match="not found"):
        validate_source_file(tmp_path / "nope.csv")


def test_empty_file_fails(tmp_path):
    with pytest.raises(SourceValidationError, match="empty"):
        validate_source_file(write(tmp_path, ""))


def test_missing_column_fails(tmp_path):
    header = HEADER.replace("TotalAmount", "Total")

    with pytest.raises(SourceValidationError, match="TotalAmount"):
        validate_source_file(write(tmp_path, f"{header}\n"))


def test_header_only_fails_row_minimum(tmp_path):
    with pytest.raises(SourceValidationError, match="0 rows"):
        validate_source_file(write(tmp_path, f"{HEADER}\n"))


def test_extra_column_is_only_a_warning(tmp_path, caplog):
    path = write(tmp_path, f"{HEADER},NewColumn\n{ROW},x\n")

    report = validate_source_file(path)

    assert report.row_count == 1
    assert "NewColumn" in caplog.text


def test_byte_order_mark_is_ignored(tmp_path):
    path = tmp_path / "bom.csv"
    path.write_text(f"{HEADER}\n{ROW}\n", encoding="utf-8-sig")

    assert validate_source_file(path).row_count == 1
