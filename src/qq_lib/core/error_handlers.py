# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import sys
from typing import NoReturn

from .config import CFG
from .error import QQNotSuitableError
from .logger import get_logger
from .repeater import Repeater

logger = get_logger(__name__)


def handle_not_suitable_error(
    exception: BaseException,
    metadata: Repeater,
) -> None:
    """
    Handle cases where a job is unsuitable for a qq operation.
    """
    # if this is the only item, print exception as an error
    if len(metadata.items) == 1:
        logger.error(exception)
        print()
        sys.exit(CFG.exit_codes.default)

    # if this is one of many items, print exception as info
    if len(metadata.items) > 1:
        logger.info(exception)

    # if all jobs were unsuitable
    if sum(
        isinstance(x, QQNotSuitableError) for x in metadata.encountered_errors.values()
    ) == len(metadata.items):
        logger.error("No suitable qq job.\n")
        sys.exit(CFG.exit_codes.default)


def handle_job_mismatch_error(
    exception: BaseException,
    _metadata: Repeater,
) -> NoReturn:
    """
    Handle cases where the provided job ID does not match the qq info file.
    """
    logger.error(exception)
    sys.exit(CFG.exit_codes.default)


def handle_general_qq_error(
    exception: BaseException,
    metadata: Repeater,
) -> None:
    """
    Handle general qq errors that occur during a qq operation.
    """
    logger.error(exception)

    # if the operation failed for all items
    if len(metadata.items) == len(metadata.encountered_errors):
        print()
        sys.exit(CFG.exit_codes.default)


def ignore_error(
    _exception: BaseException,
    _metadata: Repeater,
) -> None:
    """
    Ignore the error.
    """
    pass
