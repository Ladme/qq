# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Exception types used throughout qq.

This module defines the core qq-specific exceptions, including recoverable
errors, job-mismatch and suitability errors, and fatal or communication-related
runner errors. Each exception carries an associated exit code used by qq
commands to report failures consistently.
"""

from qq_lib.core.config import CFG

from .logger import get_logger

logger = get_logger(__name__)


class QQError(Exception):
    """Common exception type for all recoverable qq errors."""

    exit_code = CFG.exit_codes.default


class QQJobMismatchError(QQError):
    """Raised when the specified job ID does not match the qq info file."""

    pass


class QQNotSuitableError(QQError):
    """Raised when a job is unsuitable for an operation."""

    pass


class QQRunFatalError(Exception):
    """
    Raised when qq runner is unable to load a qq info file.

    Should only be used to signal that the error state cannot be logged into a qq info file.
    """

    exit_code = CFG.exit_codes.qq_run_fatal


class QQRunCommunicationError(Exception):
    """
    Raised when qq runner detects an inconsistency between the information
    it has and the information in the corresponding qq info file.
    """

    exit_code = CFG.exit_codes.qq_run_communication
