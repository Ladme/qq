# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path

import click

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import get_info_files
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.go.goer import QQGoer

logger = get_logger(__name__)


@click.command(
    short_help="Change to the qq job's working directory.",
    help="Go to the qq job's working directory, using `cd` locally or `ssh` if the directory is on a remote host.",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
def go():
    """
    Go to the working directory of the qq job submitted from this directory.
    """
    info_files = get_info_files(Path())
    if not info_files:
        logger.error("No qq job info file found.")
        sys.exit(91)

    n_suitable = 0  # number of jobs suitable to be navigated to
    n_successful = 0  # number of jobs succesfully navigated to
    for file in info_files:
        try:
            goer = QQGoer(file)
            goer.printInfo()
            if goer.isFinished():
                if len(info_files) > 1:
                    logger.info(
                        "Job has finished and was synchronized: working directory does not exist."
                    )
                    continue
                n_suitable -= 1
                # continue in the current cycle if only one info file
                # will fail in the next step and return a proper error

            if goer.isKilled() and not goer.hasDestination():
                if len(info_files) > 1:
                    logger.info(
                        "Job has been killed and no working directory is available."
                    )
                    continue
                n_suitable -= 1

            n_suitable += 1
            goer.checkAndNavigate()
            n_successful += 1
        except QQError as e:
            logger.error(e)
        except Exception as e:
            logger.critical(e, exc_info=True, stack_info=True)
            print()
            # exit always, this is a bug
            sys.exit(99)

    if n_suitable == 0 and len(info_files) > 1:
        logger.error("No qq job suitable for 'qq go'.\n")
        sys.exit(91)

    if n_successful == 0:
        print()
        sys.exit(91)

    print()
    sys.exit(0)
