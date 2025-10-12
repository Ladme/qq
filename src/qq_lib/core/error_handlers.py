# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import sys
from typing import NoReturn

from qq_lib.core.error import QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.core.repeater import QQRepeater

logger = get_logger(__name__)


def handle_not_suitable_error(
    exception: BaseException,
    metadata: QQRepeater,
) -> None:
    """
    Handle cases where a job is unsuitable for a qq operation.
    """
    # if this is the only item, print exception as an error
    if len(metadata.items) == 1:
        logger.error(exception)
        print()
        sys.exit(91)

    # if this is one of many items, print exception as info
    if len(metadata.items) > 1:
        logger.info(exception)

    # if all jobs were unsuitable
    if sum(
        isinstance(x, QQNotSuitableError) for x in metadata.encountered_errors.values()
    ) == len(metadata.items):
        logger.error("No suitable qq job.\n")
        sys.exit(91)


def handle_job_mismatch_error(
    exception: BaseException,
    _metadata: QQRepeater,
) -> NoReturn:
    """
    Handle cases where the provided job ID does not match the qq info file.
    """
    logger.error(exception)
    sys.exit(91)


def handle_general_qq_error(
    exception: BaseException,
    metadata: QQRepeater,
) -> None:
    """
    Handle general qq errors that occur during a qq operation.
    """
    logger.error(exception)

    # if the operation failed for all items
    if len(metadata.items) == len(metadata.encountered_errors):
        print()
        sys.exit(91)
