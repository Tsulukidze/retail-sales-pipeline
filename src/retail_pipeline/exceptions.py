"""Error types used by the pipeline.

Separate error types make the logs clear: we can see at once if the source
was not available or if the file was wrong. This also helps to decide if a
retry makes sense.
"""


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class SourceExtractionError(PipelineError):
    """The dataset could not be downloaded."""


class SourceValidationError(PipelineError):
    """The downloaded file does not have the expected structure."""
