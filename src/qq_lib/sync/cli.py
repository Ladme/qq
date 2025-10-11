# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
import sys
from pathlib import Path

import click
from rich.console import Console

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import get_info_files_from_job_id_or_dir
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
from qq_lib.sync.syncer import QQSyncer

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Fetch files from a job's working directory.",
    help=f"""Fetch files from the working directory of the specified qq job or
working directory (directories) of qq job(s) submitted from this directory.

{click.style("JOB_ID", fg="green")}   Identifier of the job whose working directory files should be fetched. Optional.

If JOB_ID is not specified, `qq sync` searches for qq jobs in the current directory.
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
@click.option(
    "-f",
    "--files",
    type=str,
    default=None,
    help="A colon-, comma-, or space-separated list of files to fetch from the working directory. If not specified, all files are fetched.",
)
def sync(job: str | None, files: str | None):
    """
    Fetch files from the working directory of the specified qq job or
    working directory (directories) of qq job(s) submitted from this directory.
    """
    try:
        info_files = get_info_files_from_job_id_or_dir(job)
        repeater = QQRepeater(
            info_files, _sync_job, job, re.split(r"[\s:,]+", files) if files else None
        )
        repeater.onException(QQJobMismatchError, handle_job_mismatch_error)
        repeater.onException(QQNotSuitableError, handle_not_suitable_error)
        repeater.onException(QQError, handle_general_qq_error)
        repeater.run()
        print()
        sys.exit(0)
    # QQErrors should be caught by QQRepeater
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


def _sync_job(info_file: Path, job: str | None, files: list[str] | None):
    """
    Perform synchronization of job files from a remote working directory to the local input directory.

    Args:
        info_file (Path): Path to the qq info file associated with the job.
        job (str | None): Optional job ID. If provided, it must correspond to the job id in
            `info_file`; otherwise, a `QQJobMismatchError` is raised.
        files (list[str] | None): Optional list of specific file names to synchronize.
            If not provided, all files are fetched from the job's working directory
            except those excluded by the batch system.

    Raises:
        QQJobMismatchError: If the given `job` does not match the job described in `info_file`.
        QQNotSuitableError: If the job is not in a state suitable for synchronization,
            e.g., it has already finished, is exiting successfully, has been killed while queued,
            or is queued/booting.
        QQError: If an error occurs during synchronization setup or execution.
    """

    syncer = QQSyncer(info_file)

    # check that the info file in the killer corresponds
    # to the specified job
    if job and not syncer.isJob(job):
        raise QQJobMismatchError(f"Info file for job '{job}' does not exist.")

    syncer.printInfo(console)

    # finished jobs do not have working directory
    if syncer.isFinished():
        raise QQNotSuitableError(
            "Job has finished and was synchronized: nothing to sync."
        )

    # killed jobs may not have working directory
    if syncer.isKilled() and not syncer.hasDestination():
        raise QQNotSuitableError(
            "Job has been killed and no working directory is available."
        )

    # succesfully exiting jobs do not have working directory
    if syncer.isExitingSuccessfully():
        raise QQNotSuitableError("Job is finishing successfully: nothing to sync.")

    # queued jobs do not have working directory
    if syncer.isQueued():
        raise QQNotSuitableError("Job is queued or booting: nothing to sync.")

    syncer.sync(files)
