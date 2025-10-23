# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import getpass
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import NoReturn

import click

from qq_lib.batch.interface.job import BatchJobInterface
from qq_lib.batch.interface.meta import QQBatchMeta
from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import yes_or_no_prompt
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQJobMismatchError, QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.core.repeater import QQRepeater
from qq_lib.kill.cli import kill_job

logger = get_logger(__name__)


@click.command(
    short_help="Terminate all your jobs.",
    help="""Terminate all your submitted qq jobs.

This command is only able to terminate qq jobs, all other jobs are not affected by it.""",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.option(
    "-y", "--yes", is_flag=True, help="Terminate the jobs without confirmation."
)
@click.option(
    "--force",
    is_flag=True,
    help="Terminate the jobs forcibly, ignoring their current state and without confirmation.",
)
def killall(yes: bool = False, force: bool = False) -> NoReturn:
    try:
        BatchSystem = QQBatchMeta.fromEnvVarOrGuess()
        jobs = BatchSystem.getUnfinishedBatchJobs(getpass.getuser())
        if not jobs:
            logger.info("You have no active jobs. Nothing to kill.")
            sys.exit(0)

        files = _jobs_to_paths(jobs)
        if not files:
            logger.info(
                f"You have no active qq jobs (and {len(jobs)} other jobs). Nothing to kill."
            )
            sys.exit(0)

        if (
            yes
            or force
            or yes_or_no_prompt(
                f"You have {len(files)} active qq job{'s' if len(files) > 1 else ''}. Do you want to kill {'them' if len(files) > 1 else 'it'}?"
            )
        ):
            repeater = QQRepeater(
                files,
                kill_job,
                force=force,
                yes=True,  # assume yes
                job=None,  # assume that all qq info files correspond to the currently running jobs
            )
            repeater.onException(QQJobMismatchError, _log_error_and_continue)
            repeater.onException(QQNotSuitableError, _log_error_and_continue)
            repeater.onException(QQError, _log_error_and_continue)
            repeater.run()
        else:
            logger.info("Operation aborted.")

        sys.exit(0)
    # QQErrors should be caught by QQRepeater
    except QQError as e:
        logger.error(e)
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(CFG.exit_codes.unexpected_error)


def _jobs_to_paths(jobs: Iterable[BatchJobInterface]) -> list[Path]:
    return [info_file for job in jobs if (info_file := job.getInfoFile())]


def _log_error_and_continue(
    exception: BaseException,
    _metadata: QQRepeater,
) -> None:
    """
    Log error as error and continue the execution.
    """
    logger.error(exception)
