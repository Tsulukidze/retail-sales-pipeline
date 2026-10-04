"""Run the data quality checks and report the results.

The checks themselves are SQL (sql/checks/data_quality_checks.sql). Each
check returns one row: check_name, passed, details. This module runs them,
formats a readable report, and raises an error if any check failed.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from retail_pipeline.db import DbConnection, transaction
from retail_pipeline.exceptions import DataQualityError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckResult:
    """The result of one data quality check."""

    name: str
    passed: bool
    details: str


def run_quality_checks(connection: DbConnection, checks_sql: str) -> list[CheckResult]:
    """Run the checks and return one result per check.

    A check whose "passed" value is NULL (for example, because a table is
    empty) counts as failed: "unknown" is not good enough.
    """
    with transaction(connection), connection.cursor() as cursor:
        cursor.execute(checks_sql)
        rows = cursor.fetchall()

    return [
        CheckResult(name=name, passed=passed is True, details=details)
        for name, passed, details in rows
    ]


def format_report(results: Sequence[CheckResult]) -> str:
    """Return a readable report, one line per check, with a summary at the end."""
    lines = [
        f"{'PASS' if result.passed else 'FAIL'}  {result.name}: {result.details}"
        for result in results
    ]
    passed = sum(result.passed for result in results)
    lines.append(f"{passed} of {len(results)} data quality checks passed")
    return "\n".join(lines)


def raise_if_failed(results: Sequence[CheckResult]) -> None:
    """Raise DataQualityError if any check failed, or if there are no checks at all."""
    if not results:
        raise DataQualityError("No data quality checks were run")

    failed = [result.name for result in results if not result.passed]
    if failed:
        raise DataQualityError(
            f"{len(failed)} of {len(results)} data quality checks failed: {', '.join(failed)}"
        )
