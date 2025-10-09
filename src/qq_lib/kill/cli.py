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
from qq_lib.kill.killer import QQKiller


class _QQKillJobMismatchError(QQError):
    """Raised when the specified job ID does not match the qq info file."""

    pass


class _QQKillNotSuitableError(QQError):
    """Raised when a job is unsuitable to be terminated due to its state."""

    pass


logger = get_logger(__name__)


@click.command(
    short_help="Terminate a qq job.",
    help=f"""Terminate the specified qq job or the qq job(s) in this directory.

{click.style("JOB_ID", fg="green")}   Identifier of the job to kill. Optional.

If JOB_ID is not specified, `qq kíll` searches for qq jobs in the current directory.

***

Unless the `-y` or `--force` flag is used, `qq kill` always
asks for confirmation before killing a job.

By default (without --force), `qq kill` will only attempt to kill jobs
that are queued, held, booting, or running but not yet finished or already killed.

When the --force flag is used, `qq kill` will attempt to terminate any job
regardless of its state, including jobs that are, according to the qq,
already finished or killed. This can be used to remove lingering (stuck) jobs.""",
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
    "--force", is_flag=True, help="Kill the job forcibly and without confirmation."
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
        repeater.onException(_QQKillJobMismatchError, _handle_job_mismatch_error)
        repeater.onException(_QQKillNotSuitableError, _handle_not_suitable_error)
        repeater.onException(QQError, _handle_general_qq_error)
        repeater.run()
        print()
        sys.exit(0)
    # QQErrors should be caught by QQRepeater
    except QQError as e:
        logger.error(e)
        print()
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        print()
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
        _QQKillJobIDError: If the job ID does not match the info file.
        _QQKillNotSuitableError: If the job is not suitable for termination.
        QQError: If the job cannot be killed or the qq info file cannot be updated.
    """
    killer = QQKiller(info_file, force)

    # check whether the info file in the killer corresponds
    # to the specified job
    if job and not killer.isJob(job):
        raise _QQKillJobMismatchError(f"Info file for job '{job}' does not exist.")

    killer.printInfo()

    # check whether the job can be killed
    if not killer.shouldTerminate():
        raise _QQKillNotSuitableError("Job not suitable for killing.")

    # perform the kill if confirmed
    if force or yes or killer.askForConfirm():
        # shouldUpdateInfoFile must be called before terminate
        # since terminate can update the state of the job
        should_update = killer.shouldUpdateInfoFile()
        killer.terminate()
        if should_update:
            killer.updateInfoFile()

        logger.info(f"Killed the job '{killer.getJobId()}'.")


def _handle_not_suitable_error(
    exception: BaseException,
    metadata: QQRepeater,
):
    """
    Handle cases where a job is unsuitable to be terminated.
    """
    if len(metadata.items) > 1:
        logger.info(exception)

    # if all jobs were unsuitable for kill
    if sum(
        isinstance(x, _QQKillNotSuitableError)
        for x in metadata.encountered_errors.values()
    ) == len(metadata.items):
        logger.error("No qq job suitable for 'qq kill'. Try using 'qq kill --force'.\n")
        sys.exit(91)


def _handle_job_mismatch_error(
    exception: BaseException,
    metadata: QQRepeater,
):
    """
    Handle cases where the provided job ID does not match the qq info file.
    """
    _ = metadata
    logger.error(exception)
    sys.exit(91)


def _handle_general_qq_error(
    exception: BaseException,
    metadata: QQRepeater,
):
    """
    Handle general qq errors that occur during job termination.
    """
    logger.error(exception)

    if len(metadata.items) == len(metadata.encountered_errors):
        print()
        sys.exit(91)
