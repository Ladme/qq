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
    yes_or_no_prompt,
)
from qq_lib.core.config import CFG
from qq_lib.core.error import (
    QQError,
    QQJobMismatchError,
    QQNotSuitableError,
)
from qq_lib.core.error_handlers import (
    handle_general_qq_error,
    handle_job_mismatch_error,
    handle_not_suitable_error,
)
from qq_lib.core.logger import get_logger
from qq_lib.core.repeater import Repeater
from qq_lib.wipe.wiper import Wiper

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Delete the working directory of a job.",
    help=f"""Delete the working directory of the specified qq job, or of all qq jobs in the current directory.

{click.style("JOB_ID", fg="green")}   The identifier of the job which working directory should be deleted. Optional.

If JOB_ID is not specified, `{CFG.binary_name} wipe` searches for qq jobs in the current directory.

By default, `{CFG.binary_name} wipe` prompts for confirmation before deleting the working directory.
Without the `--force` flag, it will only attempt to delete working directories of jobs that have failed or been killed.

When the `--force` flag is used, `{CFG.binary_name} wipe` attempts to wipe the working directory of any job
regardless of its state, including jobs that are queued, running or successfully finished.
You should be very careful when using this option as it may delete useful data or cause your job to crash!

If the working directory matches the input directory, `{CFG.binary_name} wipe` will never delete it, even if you use the `--force` flag.""",
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
    "-y",
    "--yes",
    is_flag=True,
    help="Delete the working directory without confirmation.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Delete the working directory of the job forcibly, ignoring its current state and without confirmation.",
)
def wipe(job: str | None, yes: bool = False, force: bool = False) -> NoReturn:
    """
    Delete the working directory of the specified qq job or qq job(s) submitted from the current directory.
    """
    try:
        info_files = get_info_files_from_job_id_or_dir(job)
        repeater = Repeater(info_files, wipe_work_dir, force, yes, job)
        repeater.onException(QQJobMismatchError, handle_job_mismatch_error)
        repeater.onException(QQNotSuitableError, handle_not_suitable_error)
        repeater.onException(QQError, handle_general_qq_error)
        repeater.run()
        print()
        sys.exit(0)
    # QQErrors should be caught by Repeater
    except QQError as e:
        logger.error(e)
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(CFG.exit_codes.unexpected_error)


def wipe_work_dir(info_file: Path, force: bool, yes: bool, job: str | None) -> None:
    """
    Attempt to delete the working directory of the job associated with the specified info file.

    Args:
        info_file (Path): Path to the qq job's info file.
        force (bool): Whether to forcibly delete the working directory regardless of the job's state.
        yes (bool): Whether to skip confirmation before deleting.
        job (str | None): Optional job ID for matching the target job.

    Raises:
        QQJobMismatchError: If the job ID does not match the info file.
        QQNotSuitableError: If the job does (or should) not have a working directory.
        QQError: If the working directory cannot be deleted.
    """
    wiper = Wiper(info_file)

    # check that the info file in the wiper corresponds
    # to the specified job
    if job and not wiper.matchesJob(job):
        raise QQJobMismatchError(f"Info file for job '{job}' does not exist.")

    wiper.printInfo(console)

    # make sure that the job is suitable for wiping
    if not force:
        wiper.ensureSuitable()

    if (
        force
        or yes
        or yes_or_no_prompt("Do you want to delete the job's working directory?")
    ):
        job_id = wiper.delete()
        logger.info(f"Deleted the working directory of the job '{job_id}'.")
    else:
        logger.info("Operation aborted.")
