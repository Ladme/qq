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
)
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.core.repeater import Repeater
from qq_lib.info.informer import Informer
from qq_lib.info.presenter import Presenter

logger = get_logger(__name__)


@click.command(
    short_help="Display information about a job.",
    help=f"""Display information about the state and properties of the specified qq job,
or of qq jobs found in the current directory.

{click.style("JOB_ID", fg="green")}   The identifier of the job to display information for. Optional.

If JOB_ID is not specified, `{CFG.binary_name} info` searches for qq jobs in the current directory.""",
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
    "-s", "--short", is_flag=True, help="Display only the job ID and current state."
)
def info(job: str | None, short: bool) -> NoReturn:
    """
    Get information about the specified qq job or qq job(s) submitted from this directory.
    """
    try:
        if job:
            informers = [Informer.fromJobId(job)]
        else:
            informers = [Informer.fromFile(info) for info in get_info_files(Path.cwd())]

        Repeater(informers, _info_for_job, short).run()
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(CFG.exit_codes.unexpected_error)


def _info_for_job(informer: Informer, short: bool) -> None:
    """
    Display information about a qq job associated with the specified Informer.

    Args:
        informer (Informer): Informer associated with the job.
        short (bool): If True, print only the job ID and the current job state.
                      If False, print the full formatted information panel.
    """
    presenter = Presenter(informer)
    console = Console()
    if short:
        console.print(presenter.getShortInfo())
    else:
        panel = presenter.createFullInfoPanel(console)
        console.print(panel)
