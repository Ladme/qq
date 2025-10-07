# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path

import click

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import get_info_files
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.kill.killer import QQKiller

logger = get_logger(__name__)


@click.command(
    short_help="Terminate the qq job.",
    help="""Terminate the qq job(s) in this directory.

Unless the `-y` or `--force` flag is used, `qq kill` always
asks for confirmation before killing a job.

By default (without --force), `qq kill` will only attempt to kill jobs
that are queued, held, booting, or running but not yet finished or already killed.

When the --force flag is used, `qq kill` will attempt to terminate any job
regardless of its state, including jobs that are, according to the qq,
already finished or killed. This can be used to remove lingering (stuck) jobs.""",
    cls=GNUHelpColorsCommand,
    help_options_color="blue",
)
@click.option(
    "-y", "--yes", is_flag=True, help="Terminate the job without confirmation."
)
@click.option(
    "--force", is_flag=True, help="Kill the job forcibly and without confirmation."
)
def kill(yes: bool = False, force: bool = False):
    """
    Terminate a qq job or multiple qq jobs submitted from the current directory.

    Unless the `-y` or `--force` flag is used, `qq kill` always
    asks for confirmation before killing a job.

    By default (without --force), `qq kill` will only attempt to kill jobs
    that are queued, held, booting, or running but not yet finished or already killed.

    When the --force flag is used, `qq kill` will attempt to terminate any job
    regardless of its state, including jobs that are, according to the qq,
    already finished or killed. This can be used to remove lingering (stuck) jobs.

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
    # get all job info files
    info_files = get_info_files(Path())
    if not info_files:
        logger.error("No qq job info file found.")
        sys.exit(91)

    n_suitable = 0  # number of jobs suitable to be killed
    n_successful_kills = 0  # number of successful kills
    for file in info_files:
        try:
            killer = QQKiller(file, force)
            killer.printInfo()

            # check whether the job can be killed
            if not killer.shouldTerminate():
                if len(info_files) > 1:
                    logger.info("Job not suitable for killing.")
                continue

            n_suitable += 1
            # perform the kill if confirmed
            if force or yes or killer.askForConfirm():
                # shouldUpdate must be called before terminate
                # since terminate can update the state of the job
                should_update = killer.shouldUpdateInfoFile()
                killer.terminate()
                if should_update:
                    killer.updateInfoFile()
                n_successful_kills += 1
                logger.info(f"Killed the job '{killer.getJobId()}'.")

        except QQError as e:
            logger.error(e)
        except Exception as e:
            logger.critical(e, exc_info=True, stack_info=True)
            print()
            # exit always, this is a bug
            sys.exit(99)

    if n_suitable == 0:
        logger.error("No qq job suitable for 'qq kill'. Try using 'qq kill --force'.\n")
        sys.exit(91)

    if n_successful_kills == 0:
        print()
        sys.exit(91)

    print()
    sys.exit(0)
