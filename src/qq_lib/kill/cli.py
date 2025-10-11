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
from qq_lib.kill.killer import QQKiller

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Terminate a job.",
    help=f"""Terminate the specified qq job, or all qq jobs in the current directory.

{click.style("JOB_ID", fg="green")}   The identifier of the job to terminate. Optional.

If JOB_ID is not specified, `qq kill` searches for qq jobs in the current directory.

By default, `qq kill` prompts for confirmation before terminating a job.
Without the `--force` flag, it will only attempt to terminate jobs that
are queued, held, booting, or running, but not yet finished or already killed.

When the `--force` flag is used, `qq kill` attempts to terminate any job regardless of its state,
including jobs that are already finished or killed.
This can be useful for removing lingering or stuck jobs.""",
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
    "-y", "--yes", is_flag=True, help="Terminate the job without confirmation."
)
@click.option(
    "--force",
    is_flag=True,
    help="Terminate the job forcibly, ignoring its current state and without confirmation.",
)
def kill(job: str | None, yes: bool = False, force: bool = False):
    """
    Terminate the specified qq job or qq job(s) submitted from the current directory.

    Details
        Killing a job sets its state to "killed". This is handled either by `qq kill` or
        `qq run`, depending on job type and whether the `--force` flag was used:

        - Forced kills: `qq kill` updates the qq info file to mark the
            job as killed, because `qq run` may not have time to do so.
            The info file is subsequently locked to avoid overwriting.

        - Jobs that have not yet started: `qq run` does not exist yet, so
            `qq kill` is responsible for marking the job as killed.

        - Jobs that are booting: `qq run` does exist for booting jobs, but
            it is unreliable at this stage. PBS's `qdel` may also silently fail for
            booting jobs. `qq kill` is thus responsible for setting the job state
            and locking the info file (which then forces `qq run` to terminate
            even if the batch system fails to kill it).

        - Normal (non-forced) termination: `qq run` is responsible for
            updating the job state in the info file once the job is terminated.
    """
    try:
        info_files = get_info_files_from_job_id_or_dir(job)
        repeater = QQRepeater(info_files, _kill_job, force, yes, job)
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


def _kill_job(info_file: Path, force: bool, yes: bool, job: str | None):
    """
    Attempt to terminate a qq job associated with the specified info file.

    Args:
        info_file (Path): Path to the qq job's info file.
        force (bool): Whether to forcibly kill the job regardless of its state.
        yes (bool): Whether to skip confirmation before termination.
        job (str | None): Optional job ID for matching the target job.

    Raises:
        QQJobMismatchError: If the job ID does not match the info file.
        QQNotSuitableError: If the job is not suitable for termination.
        QQError: If the job cannot be killed or the qq info file cannot be updated.
    """
    killer = QQKiller(info_file, force)

    # check that the info file in the killer corresponds
    # to the specified job
    if job and not killer.isJob(job):
        raise QQJobMismatchError(f"Info file for job '{job}' does not exist.")

    killer.printInfo(console)

    # check whether the job can be killed
    if not killer.shouldTerminate():
        raise QQNotSuitableError("Job is not suitable for killing.")

    # perform the kill if confirmed
    if force or yes or killer.askForConfirm():
        # shouldUpdateInfoFile must be called before terminate
        # since terminate can update the state of the job
        should_update = killer.shouldUpdateInfoFile()
        killer.terminate()
        if should_update:
            killer.updateInfoFile()

        logger.info(f"Killed the job '{killer.getJobId()}'.")
