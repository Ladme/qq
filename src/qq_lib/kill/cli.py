# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path
from typing import NoReturn

import click
from rich.console import Console

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import (
    get_info_files,
    yes_or_no_prompt,
)
from qq_lib.core.config import CFG
from qq_lib.core.error import (
    QQError,
    QQNotSuitableError,
)
from qq_lib.core.error_handlers import (
    handle_general_qq_error,
    handle_not_suitable_error,
)
from qq_lib.core.logger import get_logger
from qq_lib.core.repeater import Repeater
from qq_lib.info.informer import Informer
from qq_lib.kill.killer import Killer

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Terminate a job.",
    help=f"""Terminate the specified qq job, or all qq jobs in the current directory.

{click.style("JOB_ID", fg="green")}   The identifier of the job to terminate. Optional.

If JOB_ID is not specified, `{CFG.binary_name} kill` searches for qq jobs in the current directory.

By default, `{CFG.binary_name} kill` prompts for confirmation before terminating a job.

Without the `--force` flag, `{CFG.binary_name} kill` will only attempt to terminate jobs that
are queued, held, booting, or running, but not yet finished or already killed.
When the `--force` flag is used, `{CFG.binary_name} kill` attempts to terminate any job regardless of its state,
including jobs that are, according to qq, already finished or killed.
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
def kill(job: str | None, yes: bool = False, force: bool = False) -> NoReturn:
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
        if job:
            informers = [Informer.fromJobId(job)]
        else:
            if not (
                informers := [
                    Informer.fromFile(info) for info in get_info_files(Path.cwd())
                ]
            ):
                raise QQError("No qq job info file found.")

        repeater = Repeater(informers, kill_job, force, yes)
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


def kill_job(informer: Informer, force: bool, yes: bool) -> None:
    """
    Attempt to terminate a qq job associated with the specified informer.

    Args:
        informer (Informer): Informer associated with the job.
        force (bool): Whether to forcibly kill the job regardless of its state.
        yes (bool): Whether to skip confirmation before termination.

    Raises:
        QQNotSuitableError: If the job is not suitable for termination.
        QQError: If the job cannot be killed or the qq info file cannot be updated.
    """
    killer = Killer.fromInformer(informer)
    killer.printInfo(console)

    # make sure that the job can actually be killed
    if not force:
        killer.ensureSuitable()

    if force or yes or yes_or_no_prompt("Do you want to kill the job?"):
        job_id = killer.kill(force)
        logger.info(f"Killed the job '{job_id}'.")
    else:
        logger.info("Operation aborted.")
