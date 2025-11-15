# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
import sys
from pathlib import Path
from typing import NoReturn

import click
from rich.console import Console

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import get_info_files
from qq_lib.core.config import CFG
from qq_lib.core.error import (
    QQError,
    QQNotSuitableError,
)
from qq_lib.core.error_handlers import (
    handle_general_qq_error,
    handle_not_suitable_error,
)
from qq_lib.core.logger import get_logger
from qq_lib.core.repeater import Repeater
from qq_lib.info.informer import Informer

from .syncer import Syncer

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Fetch files from a job's working directory.",
    help=f"""Fetch files from the working directory of the specified qq job, or from the
working directory of the job submitted from the current directory.

{click.style("JOB_ID", fg="green")}   The identifier of the job whose working directory files should be fetched. Optional.

If JOB_ID is not specified, `{CFG.binary_name} sync` searches for qq jobs in the current directory.
If multiple suitable jobs are found, `{CFG.binary_name} sync` fetches files from each job in turn.
Files fetched from later jobs may overwrite files from earlier jobs in the input directory.

Files are copied from the job's working directory to its input directory, not to the current directory.
""",
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
    "-f",
    "--files",
    type=str,
    default=None,
    help="""A colon-, comma-, or space-separated list of files or directories to fetch.
If not specified, the entire content of the working directory is fetched.""",
)
def sync(job: str | None, files: str | None) -> NoReturn:
    """
    Fetch files from the working directory of the specified qq job or
    working directory (directories) of qq job(s) submitted from this directory.
    """
    try:
        if job:
            informers = [Informer.fromJobId(job)]
        else:
            informers = [Informer.fromFile(info) for info in get_info_files(Path.cwd())]

        repeater = Repeater(informers, _sync_job, _split_files(files))
        repeater.onException(QQNotSuitableError, handle_not_suitable_error)
        repeater.onException(QQError, handle_general_qq_error)
        repeater.run()
        print()
        sys.exit(0)
    # QQErrors should be caught by Repeater
    except QQError as e:
        logger.error(e)
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(CFG.exit_codes.unexpected_error)


def _split_files(files: str | None) -> list[str] | None:
    """
    Split the list of files provided on the command line.
    """
    if not files:
        return None

    return re.split(r"[\s:,]+", files)


def _sync_job(informer: Informer, files: list[str] | None) -> None:
    """
    Perform synchronization of job files from a remote working directory to the local input directory.

    Args:
        informer (Informer): Informer associated with the job.
        files (list[str] | None): Optional list of specific file names to synchronize.
            If not provided, all files are fetched from the job's working directory
            except those excluded by the batch system.

    Raises:
        QQNotSuitableError: If the job is not in a state suitable for synchronization,
            e.g., it has already finished, is exiting successfully, has been killed while queued,
            or is queued/booting.
        QQError: If an error occurs during synchronization setup or execution.
    """

    syncer = Syncer.fromInformer(informer)
    syncer.printInfo(console)

    # make sure that the job is suitable to be synced
    syncer.ensureSuitable()

    syncer.sync(files)
