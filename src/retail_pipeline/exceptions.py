"""Error types used by the pipeline.

Separate error types make the logs clear: they show at once if the source
was not available or if the file was wrong. This also helps to decide if a
retry makes sense.
"""


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class SourceExtractionError(PipelineError):
    """The dataset could not be downloaded."""


class SourceValidationError(PipelineError):
    """The downloaded file does not have the expected structure."""


class RowParsingError(PipelineError):
    """One value in a source row has the wrong format."""

    def __init__(self, column: str, value: str | None, problem: str) -> None:
        """Describe the problem with the value of `column`."""
        super().__init__(f"{column}={value!r}: {problem}")
        self.column = column
        self.value = value


class StagingLoadError(PipelineError):
    """The staging table does not contain the rows that were sent."""


class DataQualityError(PipelineError):
    """One or more data quality checks did not pass."""
