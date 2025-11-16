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
from qq_lib.go.goer import Goer
from qq_lib.info.informer import Informer

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Open a shell in a job's working directory.",
    help=f"""Open a new shell in the working directory of the specified qq job, or in the
working directory of the job submitted from the current directory.

{click.style("JOB_ID", fg="green")}   The identifier of the job whose working directory should be entered. Optional.

If JOB_ID is not specified, `{CFG.binary_name} go` searches for qq jobs in the current directory.
If multiple suitable jobs are found, `{CFG.binary_name} go` opens a shell for each job in turn.

Uses `cd` for local directories or `ssh` if the working directory is on a remote host.
Note that this command does not change the working directory of the current shell;
it always opens a new shell at the destination.
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
def go(job: str | None) -> NoReturn:
    """
    Go to the working directory (directories) of the specified qq job or qq job(s) submitted from this directory.
    """
    try:
        if job:
            informers = [Informer.fromJobId(job)]
        else:
            if not (
                informers := [
                    Informer.fromFile(info) for info in get_info_files(Path.cwd())
                ]
            ):
                raise QQError("No qq job info file found.")

        repeater = Repeater(informers, _go_to_job)
        repeater.onException(QQNotSuitableError, handle_not_suitable_error)
        repeater.onException(QQError, handle_general_qq_error)
        repeater.run()
        print()
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(CFG.exit_codes.unexpected_error)


def _go_to_job(informer: Informer) -> None:
    """
    Navigate to the working directory of a qq job if it is accessible.

    Args:
        informer (Informer): Informer associated with the job.

    Raises:
        QQError: If the navigation fails.
    """
    goer = Goer.fromInformer(informer)
    goer.printInfo(console)

    # make sure that the job is not in a state without a working directory
    goer.ensureSuitable()

    # navigate to the working directory
    goer.go()
