# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import getpass
import sys
from typing import NoReturn

import click
from rich.console import Console

from qq_lib.batch.interface import BatchMeta
from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.jobs.presenter import JobsPresenter

logger = get_logger(__name__)


@click.command(
    short_help="Display a summary of a user's jobs.",
    help="Display a summary of your jobs or those of a specified user. By default, only unfinished jobs are shown.",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.option(
    "-u",
    "--user",
    type=str,
    default=None,
    help="Username whose jobs should be displayed. Defaults to your own username.",
)
@click.option(
    "-e",
    "--extra",
    is_flag=True,
    help="Show additional information about the jobs.",
)
@click.option(
    "-a",
    "--all",
    is_flag=True,
    help="Include both unfinished and finished jobs in the summary.",
)
@click.option("--yaml", is_flag=True, help="Output job metadata in YAML format.")
def jobs(user: str, extra: bool, all: bool, yaml: bool) -> NoReturn:
    try:
        batch_system = BatchMeta.fromEnvVarOrGuess()
        if not user:
            # use the current user, if `--user` is not specified
            user = getpass.getuser()

        if all:
            jobs = batch_system.getBatchJobs(user)
        else:
            jobs = batch_system.getUnfinishedBatchJobs(user)

        if not jobs:
            logger.info("No jobs found.")
            sys.exit(0)

        batch_system.sortJobs(jobs)
        presenter = JobsPresenter(batch_system, jobs, extra, all)
        if yaml:
            presenter.dumpYaml()
        else:
            console = Console(record=False, markup=False)
            panel = presenter.createJobsInfoPanel(console)
            console.print(panel)

        sys.exit(0)
    except QQError as e:
        logger.error(e)
        print()
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        print()
        sys.exit(CFG.exit_codes.unexpected_error)
