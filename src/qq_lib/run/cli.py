# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import os
import sys
from pathlib import Path
from typing import NoReturn

import click

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQRunCommunicationError, QQRunFatalError
from qq_lib.core.logger import get_logger

from .runner import QQRunner, log_fatal_error_and_exit

logger = get_logger(__name__)  # intentionally does not show datetime


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
    copy of the job. The original script copied to the working directory
    is used instead.

    Exits:
        Exits with the script's exit code, or with specific
        error codes:
            90: Script not being run inside a qq environment.
            91: Failed with a standard qq error logged into an info file.
            92: Fatal error not logged into an info file.
            93: Job killed without qq run being notified.
            99: Fatal unexpected error (indicates a bug).

        In case the execution is terminated by SIGTERM or SIGKILL,
        a specific value of the exit code cannot be guaranteed
        because it is typically set by the batch system itself
        (PBS uses 256 + signal number).
    """

    # the script path provided points to a script copied to a temporary
    # location by the batch system => we ignore it and later use the
    # 'original' script in the working directory
    _ = script_path

    try:
        # make sure that qq run is being run as a batch job
        ensureQQEnv()
    except Exception as e:
        logger.error(e)
        sys.exit(CFG.exit_codes.not_qq_env)

    try:
        # get the destination of the info file from env vars
        if not (info_file := os.environ.get(CFG.env_vars.info_file)):
            raise QQRunFatalError(
                f"'{CFG.env_vars.info_file}' environment variable is not set."
            )

        if not (input_machine := os.environ.get(CFG.env_vars.input_machine)):
            raise QQRunFatalError(
                f"'{CFG.env_vars.input_machine}' environment variable is not set."
            )

        # initialize the runner
        runner = QQRunner(Path(info_file), input_machine)

        # prepare the working directory
        runner.prepare()

        # execute the script
        exit_code = runner.execute()

        # finalize the execution and clean up the work dir
        runner.finalize()

        sys.exit(exit_code)
    except QQRunFatalError as e:
        # if the execution fails with a fatal qq error, info file is not available
        # only log to stderr
        log_fatal_error_and_exit(e)  # exits here
    except QQRunCommunicationError as e:
        # miscommunication error - the info file state is not consistent
        # with QQRunner's expectations - do not update it
        log_fatal_error_and_exit(e)  # exits here
    except Exception as e:
        # other exceptions should be logged into both stderr and the info file
        runner.logFailureAndExit(e)  # exits here


def ensureQQEnv() -> None:
    """
    Raises an exception if the script is not running inside qq environment.
    """
    if not os.environ.get(CFG.env_vars.guard):
        raise QQError(
            "This script must be run as a qq job within the batch system. "
            f"To submit it properly, use: '{CFG.binary_name} submit'."
        )
