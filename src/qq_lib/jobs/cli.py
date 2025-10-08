# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import getpass
import sys

import click
from rich.console import Console

from qq_lib.batch.interface import QQBatchMeta
from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.jobs.presenter import QQJobsPresenter

logger = get_logger(__name__)


@click.command(
    short_help="See a summary of your jobs.",
    help="See a summary of your or someone else's jobs. Only unfinished jobs are shown by default.",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.option(
    "-u",
    "--user",
    type=str,
    default=None,
    help="Name of the user whose jobs should be displayed. Defaults to your username.",
)
@click.option(
    "-a", "--all", is_flag=True, help="Show both unfinished and finished jobs."
)
def jobs(user: str, all: bool):
    try:
        BatchSystem = QQBatchMeta.fromEnvVarOrGuess()
        if not user:
            # use the current user, if `--user` is not specified
            user = getpass.getuser()

        if all:
            jobs = BatchSystem.getJobsInfo(user)
        else:
            jobs = BatchSystem.getUnfinishedJobsInfo(user)

        if not jobs:
            logger.info("No jobs found.")
            return

        presenter = QQJobsPresenter(jobs)
        console = Console()
        panel = presenter.createJobsInfoPanel(console)
        console.print(panel)

        sys.exit(0)
    except QQError as e:
        logger.error(e)
        print()
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        print()
        sys.exit(99)
