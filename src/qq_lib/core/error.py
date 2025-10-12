# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from .logger import get_logger

logger = get_logger(__name__)


class QQError(Exception):
    """Common exception type for all qq errors."""

    pass


class QQJobMismatchError(QQError):
    """Raised when the specified job ID does not match the qq info file."""

    pass


class QQNotSuitableError(QQError):
    """Raised when a job is unsuitable for an operation."""

    pass
