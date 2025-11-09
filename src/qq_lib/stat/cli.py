# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import sys
from typing import NoReturn

import click
from rich.console import Console

from qq_lib.batch.interface import BatchMeta
from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.jobs.presenter import QQJobsPresenter

logger = get_logger(__name__)


@click.command(
    short_help="Display a summary of all users' jobs.",
    help="Display a summary of jobs from all users. By default, only unfinished jobs are shown.",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
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
def stat(extra: bool, all: bool, yaml: bool) -> NoReturn:
    try:
        BatchSystem = BatchMeta.fromEnvVarOrGuess()

        if all:
            jobs = BatchSystem.getAllBatchJobs()
        else:
            jobs = BatchSystem.getAllUnfinishedBatchJobs()

        if not jobs:
            logger.info("No jobs found.")
            sys.exit(0)

        BatchSystem.sortJobs(jobs)
        presenter = QQJobsPresenter(jobs, extra)
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
