# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path
from typing import NoReturn

import click

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

from .clearer import Clearer

logger = get_logger(__name__)


@click.command(
    short_help="Delete qq runtime files from the current directory.",
    help=f"""Delete qq runtime files from the current directory.

By default, `{CFG.binary_name} clear` removes only those files that do not correspond to an active or successfully completed job.
To force deletion of all qq files regardless of job status, use the `--force` flag.""",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force deletion of all qq runtime files, even if jobs are active or successfully completed.",
    default=False,
)
def clear(force: bool) -> NoReturn:
    """
    Delete qq runtime files in the current directory.
    """
    try:
        clearer = Clearer(Path())
        clearer.clear(force)
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(CFG.exit_codes.unexpected_error)
