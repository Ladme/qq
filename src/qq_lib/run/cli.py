# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import sys
from typing import NoReturn

import click

from qq_lib.core.error import QQError
from qq_lib.core.guard import guard
from qq_lib.core.logger import get_logger

from .runner import QQRunner, log_fatal_qq_error, log_fatal_unexpected_error

logger = get_logger(__name__, show_time=True)


@click.command(
    hidden=True,
    help=f"Execute a script inside the qq environment. {click.style('Do not run directly!', fg='red')}",
)
@click.argument("script_path", type=str, metavar=click.style("SCRIPT"))
def run(script_path: str) -> NoReturn:
    """
    Entrypoint for executing a script inside the qq batch environment.

    - Ensures the script is running in a batch job context
    - Prepares the job working directory (scratch or shared)
    - Executes the script and handles exit codes
    - Logs errors or unexpected failures into the qq info file

    Note that the 'script_path' provided here is ignored.
    That's because the batch system provides only a temporary
    copy of the job. The original script in the working directory
     is used instead.

    Exits:
        Exits with the script's exit code, or with specific
        error codes:
            91: Guard check failure or an error logged into an info file
            92: Fatal error not logged into an info file
            93: Job killed without qq run being notified.
            99: Fatal unexpected error (indicates a bug)

        In case the execution is terminated by SIGTERM or SIGKILL,
        a specific value of the exit code cannot be guaranteed
        because it is typically set by the batch system itself
        (PBS uses 256 + signal number).
    """

    # the script path provided points to a script copied to a temporary
    # location by the batch system => we ignore it and later use the
    # 'original' script in the working directory
    _ = script_path

    # make sure that qq run is being run as a batch job
    try:
        guard()
    except Exception as e:
        logger.error(e)
        sys.exit(91)

    # initialize the runner performing only the most necessary operations
    try:
        runner = QQRunner()
    except QQError as e:
        # the most basic setup of the run failed
        # can't even log the failure state to the info file
        log_fatal_qq_error(e, 92)  # exits here
    except Exception as e:
        log_fatal_unexpected_error(e, 99)  # exits here

    # prepare the working directory, execute the script and perform clean-up
    try:
        runner.setUp()
        runner.setUpWorkDir()
        exit_code = runner.executeScript()
        runner.finalize()
        sys.exit(exit_code)
    except QQError as e:
        # if the execution fails, log this error into both stderr and the info file
        logger.error(e)
        runner.logFailureIntoInfoFile(91)  # exits here
    except Exception as e:
        # even unknown exceptions should be logged into both stderr and the info file
        # this indicates a bug in the program
        logger.critical(e, exc_info=True, stack_info=True)
        runner.logFailureIntoInfoFile(99)  # exits here
