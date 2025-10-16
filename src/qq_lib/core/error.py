# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from .logger import get_logger

logger = get_logger(__name__)


class QQError(Exception):
    """Common exception type for all recoverable qq errors."""

    exit_code = 91


class QQJobMismatchError(QQError):
    """Raised when the specified job ID does not match the qq info file."""

    pass


class QQNotSuitableError(QQError):
    """Raised when a job is unsuitable for an operation."""

    pass


class QQRunFatalError(Exception):
    """
    Raised when qq runner is unable to load a qq info file
    or if qq run is being called outside of qq environment.

    Should only be used to signal that the error state cannot be logged into a qq info file.
    """

    exit_code = 92


class QQRunCommunicationError(Exception):
    """
    Raised when qq runner detects an inconsistency between the information
    it has and the information in the corresponding qq info file.
    """

    exit_code = 93
