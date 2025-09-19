# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import os
import sys
from pathlib import Path

import click

from qq_lib.common import QQ_SUFFIXES, get_files_with_suffix
from qq_lib.logger import get_logger

logger = get_logger("qq clear", True)


@click.command()
def clear():
    """
    Prepare the current directory for submitting a qq job.
    """
    try:
        clear_files(Path("."))
        sys.exit(0)
    except Exception as e:
        logger.critical(e)
        sys.exit(1)


def clear_files(directory: Path):
    for suffix in QQ_SUFFIXES:
        for file in get_files_with_suffix(directory, suffix):
            logger.debug(f"Removing file '{file}'.")
            os.remove(file)
