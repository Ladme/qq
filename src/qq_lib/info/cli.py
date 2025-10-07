# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path

import click
from rich.console import Console

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import get_info_files
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import QQInformer
from qq_lib.info.presenter import QQPresenter

logger = get_logger(__name__)


@click.command(
    short_help="Get information about the qq job.",
    help="Get information about the state and properties of the qq job(s) in this directory.",
    cls=GNUHelpColorsCommand,
    help_options_color="blue",
)
@click.option(
    "-s", "--short", is_flag=True, help="Print only the job ID and the current state."
)
def info(short: bool):
    """
    Get information about the qq job submitted from this directory.
    """
    info_files = get_info_files(Path())
    if not info_files:
        logger.error("No qq job info file found.")
        sys.exit(91)

    try:
        for file in info_files:
            presenter = QQPresenter(QQInformer.fromFile(file))
            console = Console()
            if short:
                console.print(presenter.getShortInfo())
            else:
                panel = presenter.createFullInfoPanel(console)
                console.print(panel)
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)
