# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import sys
from typing import NoReturn

import click

from qq_lib.batch.interface import QQBatchMeta
from qq_lib.cd.cder import QQCder
from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)


@click.command(
    short_help="Change to a job's input directory.",
    help=f"""Change the current working directory to the input directory of the specified job.

{click.style("JOB_ID", fg="green")}   The identifier of the job whose input directory should be entered.

Note that this command does not open a new shell.
""",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.argument(
    "job",
    type=str,
    metavar=click.style("JOB_ID", fg="green"),
)
def cd(job: str) -> NoReturn:
    """
    This command gets the input directory for the specified job
    and prints it. A bash qq cd function should be set up
    which then cds to this directory in the parent shell.
    """
    try:
        cder = QQCder(QQBatchMeta.fromEnvVarOrGuess(), job)
        print(cder.cd())
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(CFG.exit_codes.unexpected_error)
