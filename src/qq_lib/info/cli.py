# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path

import click
from rich.console import Console

from qq_lib.batch.interface import BatchJobInfoInterface, QQBatchInterface, QQBatchMeta
from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import get_info_files
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import QQInformer
from qq_lib.info.presenter import QQPresenter

logger = get_logger(__name__)


@click.command(
    short_help="Get information about a qq job.",
    help=f"""Get information about the state and properties of the specified qq job or qq job(s) in the current directory.

{click.style("JOB_ID", fg="green")}   Identifier of the job to get info for. Optional.

If JOB_ID is not specified, 'qq info' searches for qq jobs in the current directory.
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
    "-s", "--short", is_flag=True, help="Print only the job ID and the current state."
)
def info(job: str, short: bool):
    """
    Get information about the qq job submitted from this directory.
    """
    try:
        if job:
            info_file = _get_info_file_from_job_id(QQBatchMeta.fromEnvVarOrGuess(), job)
            # check that the detected info file exists
            if not info_file.is_file():
                raise QQError(
                    f"Info file for job '{job}' does not exist or is not reachable."
                )
            info_files = [info_file]
        else:
            # get info files from the directory
            info_files = get_info_files(Path())
            if not info_files:
                raise QQError("No qq job info file found.")

        for file in info_files:
            informer = QQInformer.fromFile(file)
            # if job id is provided on the command line,
            # we need to check that the info file actually corresponds to this job
            if job and not informer.isJob(job):
                raise QQError(
                    f"Info file for job '{job}' does not exist or is not reachable."
                )

            presenter = QQPresenter(QQInformer.fromFile(file))
            console = Console()
            if short:
                console.print(presenter.getShortInfo())
            else:
                panel = presenter.createFullInfoPanel(console)
                console.print(panel)
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


def _get_info_file_from_job_id(
    BatchSystem: type[QQBatchInterface], job_id: str
) -> Path:
    """
    Get path to the qq info file corresponding to a job with the given ID.

    Args:
        BatchSystem (type[QQBatchInterface]): The batch system class to use.
        job_id (str): The ID of the job for which to retrieve the info file.

    Returns:
        Path: Absolute path to the QQ job information file.

    Raises:
        QQError: If the job does not exist or is not a qq job.
    """

    job_info: BatchJobInfoInterface = BatchSystem.getJobInfo(job_id)
    if job_info.isEmpty():
        raise QQError(f"Job '{job_id}' does not exist.")

    if not (path := job_info.getInfoFile()):
        raise QQError(f"Job '{job_id}' is not a qq job.")

    return path
