# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path

import click

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

from .clearer import QQClearer

logger = get_logger(__name__)


@click.command(
    short_help="Delete qq run files.",
    help="""Delete qq run files from the current directory.

By default, `qq clear` only removes files if the directory does not contain an active or successfully completed job.
To force deletion of the files regardless of job status, use the `--force` flag.""",
    cls=GNUHelpColorsCommand,
    help_options_color="blue",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force deletion of all qq run files, even if jobs are active or successfully completed.",
    default=False,
)
def clear(force: bool):
    """
    Delete all qq run files in the current directory.
    """
    try:
        clearer = QQClearer(Path())
        files = clearer.getQQFiles()
        clearer.clearFiles(files, force)
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)
