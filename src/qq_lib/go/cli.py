# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path

import click
from rich.console import Console

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import (
    get_info_files_from_job_id_or_dir,
)
from qq_lib.core.error import (
    QQError,
    QQJobMismatchError,
    QQNotSuitableError,
    handle_general_qq_error,
    handle_job_mismatch_error,
    handle_not_suitable_error,
)
from qq_lib.core.logger import get_logger
from qq_lib.core.repeater import QQRepeater
from qq_lib.go.goer import QQGoer

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Open a shell in a job's working directory.",
    help=f"""Open a new shell in the working directory of the specified qq job, or in the
working directory of the job submitted from the current directory.

{click.style("JOB_ID", fg="green")}   The identifier of the job whose working directory should be entered. Optional.

If JOB_ID is not specified, `qq go` searches for qq jobs in the current directory.
If multiple suitable jobs are found, `qq go` opens a shell for each job in turn.

Uses `cd` for local directories or `ssh` if the working directory is on a remote host.
Note that this command does not change the working directory of the current shell;
it always opens a new shell at the destination.
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
        repeater.onException(QQJobMismatchError, handle_job_mismatch_error)
        repeater.onException(QQNotSuitableError, handle_not_suitable_error)
        repeater.onException(QQError, handle_general_qq_error)
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
        QQJobMismatchError: If the info file does not correspond to the specified job.
        QQNotSuitableError: If the job has finished & been synchronized or has been killed and
                            has no working directory.
        QQError: If the navigation fails for a different reason.
    """
    goer = QQGoer(info_file)

    # check thatthe info file in the goer corresponds
    # to the specified job
    if job and not goer.isJob(job):
        raise QQJobMismatchError(
            f"Info file for job '{job}' does not exist or is not reachable."
        )

    goer.printInfo(console)

    # finished jobs do not have a working directory
    if goer.isFinished():
        raise QQNotSuitableError(
            "Job has finished and was synchronized: working directory does not exist."
        )

    # killed jobs may not have a working directory
    if goer.isKilled() and not goer.hasDestination():
        raise QQNotSuitableError(
            "Job has been killed and no working directory is available."
        )

    # go to the working directory
    goer.checkAndNavigate()
