import pytest

from retail_pipeline.exceptions import DataQualityError
from retail_pipeline.quality import (
    CheckResult,
    format_report,
    raise_if_failed,
    run_quality_checks,
)


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql):
        self.executed = sql

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, rows):
        self.fake_cursor = FakeCursor(rows)
        self.commits = 0

    def cursor(self):
        return self.fake_cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass


def test_run_quality_checks_turns_rows_into_results():
    connection = FakeConnection([("check_a", True, "fine"), ("check_b", False, "3 bad rows")])

    results = run_quality_checks(connection, "SELECT ...")

    assert results == [
        CheckResult("check_a", True, "fine"),
        CheckResult("check_b", False, "3 bad rows"),
    ]
    assert connection.fake_cursor.executed == "SELECT ..."


def test_null_result_counts_as_failed():
    # An empty table can make the SQL comparison NULL instead of true/false.
    connection = FakeConnection([("check_on_empty_table", None, "0 rows")])

    results = run_quality_checks(connection, "SELECT ...")

    assert results[0].passed is False


def test_format_report_shows_every_check_and_a_summary():
    results = [CheckResult("check_a", True, "fine"), CheckResult("check_b", False, "3 bad rows")]

    report = format_report(results)

    assert report.splitlines() == [
        "PASS  check_a: fine",
        "FAIL  check_b: 3 bad rows",
        "1 of 2 data quality checks passed",
    ]


def test_raise_if_failed_does_nothing_when_all_pass():
    raise_if_failed([CheckResult("check_a", True, "fine")])


def test_raise_if_failed_names_the_failed_checks():
    results = [
        CheckResult("check_a", True, "fine"),
        CheckResult("check_b", False, "bad"),
        CheckResult("check_c", False, "bad"),
    ]

    with pytest.raises(DataQualityError, match=r"2 of 3 .* check_b, check_c"):
        raise_if_failed(results)


def test_raise_if_failed_when_no_checks_ran():
    with pytest.raises(DataQualityError, match="No data quality checks"):
        raise_if_failed([])
