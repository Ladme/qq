# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path
from typing import NoReturn

import click
from rich.console import Console

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import (
    get_info_files_from_job_id_or_dir,
)
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.core.repeater import QQRepeater
from qq_lib.info.informer import QQInformer
from qq_lib.info.presenter import QQPresenter

logger = get_logger(__name__)


@click.command(
    short_help="Display information about a job.",
    help=f"""Display information about the state and properties of the specified qq job,
or of qq jobs found in the current directory.

{click.style("JOB_ID", fg="green")}   The identifier of the job to display information for. Optional.

If JOB_ID is not specified, `qq info` searches for qq jobs in the current directory.""",
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
    "-s", "--short", is_flag=True, help="Display only the job ID and current state."
)
def info(job: str | None, short: bool) -> NoReturn:
    """
    Get information about the specified qq job or qq job(s) submitted from this directory.
    """
    try:
        info_files = get_info_files_from_job_id_or_dir(job)
        QQRepeater(info_files, _info_for_job, short, job).run()
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


def _info_for_job(info_file: Path, short: bool, job: str | None) -> None:
    """
    Display information about a qq job based on its info file and the batch system information.

    Args:
        info_file (Path): Path to the qq job's info file.
        short (bool): If True, print only the job ID and the current job state.
                      If False, print the full formatted information panel.
        job (str | None): Optional job ID to verify against the info file.

    Raises:
        QQError: If the provided job ID does not match the job in the info file.
    """
    informer = QQInformer.fromFile(info_file)

    # if job id is provided on the command line,
    # we need to check that the info file actually corresponds to this job
    if job and not informer.isJob(job):
        raise QQError(f"Info file for job '{job}' does not exist or is not reachable.")

    presenter = QQPresenter(informer)
    console = Console()
    if short:
        console.print(presenter.getShortInfo())
    else:
        panel = presenter.createFullInfoPanel(console)
        console.print(panel)
