# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path

import click

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import (
    get_info_files_from_job_id_or_dir,
)
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.core.repeater import QQRepeater
from qq_lib.go.goer import QQGoer


class _QQGoJobMismatchError(QQError):
    """Raised when the specified job ID does not match the qq info file."""

    pass


class _QQGoNotSuitableError(QQError):
    """Raised when a job is unsuitable for qq go due to its state."""

    pass


logger = get_logger(__name__)


@click.command(
    short_help="Change to the qq job's working directory.",
    help=f"""Go to the working directory of the specified qq job or to the working directory
(directories) of job(s) submitted from this directory.

{click.style("JOB_ID", fg="green")}   Identifier of the job whose working directory should be visited. Optional.

If JOB_ID is not specified, `qq go` searches for qq jobs in the current directory.

***

Uses `cd` locally or `ssh` if the working directory is on a remote host.
No matter the employed method, this always opens a new shell at the destination.
""",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.argument(
    "job",
    type=str,
    metavar=click.style("JOB_ID", fg="green"),
    required=False,
    default=None,
)
def go(job: str | None):
    """
    Go to the working directory (directories) of the specified qq job or qq job(s) submitted from this directory.
    """
    try:
        info_files = get_info_files_from_job_id_or_dir(job)
        repeater = QQRepeater(info_files, _go_to_job, job)
        repeater.onException(_QQGoJobMismatchError, _handle_job_mismatch_error)
        repeater.onException(_QQGoNotSuitableError, _handle_not_suitable_error)
        repeater.onException(QQError, _handle_general_qq_error)
        repeater.run()
        print()
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


def _go_to_job(info_file: Path, job: str | None):
    """
    Navigate to the working directory of a qq job if it is accessible.

    Args:
        info_file (Path): Path to the qq job's info file.
        job (str | None): Optional job ID to verify against the info file.

    Raises:
        _QQGoJobMismatchError: If the info file does not correspond to the specified job.
        _QQGoNotSuitableError: If the job has finished & been synchronized or has been killed and
                               has no working directory.
        QQError: If the navigation fails for a different reason.
    """
    goer = QQGoer(info_file)

    # check whether the info file in the goer corresponds
    # to the specified job
    if job and not goer.isJob(job):
        raise _QQGoJobMismatchError(
            f"Info file for job '{job}' does not exist or is not reachable."
        )

    goer.printInfo()

    # finished jobs do not have a working directory
    if goer.isFinished():
        raise _QQGoNotSuitableError(
            "Job has finished and was synchronized: working directory does not exist."
        )

    # killed jobs may not have a working directory
    if goer.isKilled() and not goer.hasDestination():
        raise _QQGoNotSuitableError(
            "Job has been killed and no working directory is available."
        )

    # go to the working directory
    goer.checkAndNavigate()


def _handle_not_suitable_error(
    exception: BaseException,
    metadata: QQRepeater,
):
    """
    Handle cases where a job is unsuitable for qq go.
    """
    # if this is the only item, print exception as an error
    if len(metadata.items) == 1:
        logger.error(exception)
        print()
        sys.exit(91)

    # if this is one of many items, print exception as info
    if len(metadata.items) > 1:
        logger.info(exception)

    # if all jobs were unsuitable for qq go
    if sum(
        isinstance(x, _QQGoNotSuitableError)
        for x in metadata.encountered_errors.values()
    ) == len(metadata.items):
        logger.error("No qq job suitable for 'qq go'.\n")
        sys.exit(91)


def _handle_job_mismatch_error(
    exception: BaseException,
    _metadata: QQRepeater,
):
    """
    Handle cases where the provided job ID does not match the qq info file.
    """
    logger.error(exception)
    sys.exit(91)


def _handle_general_qq_error(
    exception: BaseException,
    metadata: QQRepeater,
):
    """
    Handle general qq errors that occur during qq go.
    """
    logger.error(exception)

    # if the operation failed for all items
    if len(metadata.items) == len(metadata.encountered_errors):
        print()
        sys.exit(91)
