# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import socket
import sys
from pathlib import Path
from time import sleep

import click
from rich.console import Console

from qq_lib.click_format import GNUHelpColorsCommand
from qq_lib.common import get_info_files
from qq_lib.constants import GOER_WAIT_TIME
from qq_lib.error import QQError
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger
from qq_lib.states import RealState

logger = get_logger(__name__)
console = Console()


@click.command(
    short_help="Change to the qq job's working directory.",
    help="Go to the qq job's working directory, using `cd` locally or `ssh` if the directory is on a remote host.",
    cls=GNUHelpColorsCommand,
    help_options_color="blue",
)
def go():
    """
    Go to the working directory of the qq job submitted from this directory.
    """
    info_files = get_info_files(Path())
    if not info_files:
        logger.error("No qq job info file found.\n")
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


class QQGoer:
    """
    Provides utilities to navigate to the working directory of a qq job
    submitted from the current directory.
    """

    def __init__(self, info_file: Path):
        """
        Initialize a QQGoer instance for a given directory.

        Args:
            info_file (Path): Path to the qq info file.

        Notes:
            Reads the qq info file and sets up the initial state and destination.
        """
        self._info_file = info_file
        self.update()

    def printInfo(self):
        """
        Display the current job status using a formatted panel.
        """
        panel = self._informer.createJobStatusPanel(console)
        console.print(panel)

    def update(self):
        """
        Refresh internal state from the QQ info file.
        """
        self._informer = QQInformer.fromFile(self._info_file)
        self._batch_system = self._informer.info.batch_system
        self._state = self._informer.getRealState()
        self._setDestination()

    def checkAndNavigate(self):
        """
        Check the job state and navigate to the working directory if appropriate.

        Behavior:
            - If already in the working directory, logs info and returns.
            - Raises QQError if the job has finished and synchronized (working directory does not exist).
            - Logs warnings if the job failed or was killed (working directory may not exist).
            - If the job is queued, retries until the job leaves the queue, updating the state every 5 seconds.
            - Navigates to the working directory for running jobs.

        Raises:
            QQError: If navigation to the working directory fails.
        """
        if self._isInWorkDir():
            logger.info("You are already in the working directory.")
            return

        if self.isFinished():
            raise QQError(
                "Job has finished and was synchronized: working directory does not exist."
            )
        if self._isFailed():
            logger.warning(
                "Job has finished with an error code: working directory may no longer exist."
            )
        elif self._isKilled():
            logger.warning("Job has been killed: working directory may not exist.")
        elif self._isQueued():
            logger.warning(
                f"Job is {str(self._state)}: working directory does not yet exist. Will retry every {GOER_WAIT_TIME} seconds."
            )
            # keep retrying until the job gets run
            while self._isQueued():
                sleep(GOER_WAIT_TIME)
                self.update()

                if self._isInWorkDir():
                    logger.info("You are already in the working directory.")
                    return
        elif self._isRunning():
            pass
        else:
            logger.warning("Job is in an unknown, unrecognized, or inconsistent state.")

        # navigate to the working directory
        self._navigate()

    def _navigate(self):
        """
        Navigate to the job's working directory using batch system-specific commands.

        Raises:
            QQError: If host or directory are undefined, or if navigation fails.
        """
        if not self._directory or not self._host:
            raise QQError(
                "Host ('main_node') or working directory ('work_dir') are not defined."
            )

        logger.info(f"Navigating to '{str(self._directory)}' on '{self._host}'.")
        self._batch_system.navigateToDestination(self._host, self._directory)

    def _isInWorkDir(self) -> bool:
        """
        Check if the current process is already in the job's working directory.

        Returns:
            bool: True if the current directory matches the job's work_dir and:
              a) either a job_dir was used to run the job, or
              b) local hostname matches the job's main node
        """
        # note that we cannot just compare directory paths, since
        # the same directory path may point to different directories
        # on the current machine and on the execution node
        # we also need to check that
        #   a) job was running in shared storage or
        #   b) we are on the same machine
        return (
            self._directory is not None
            and Path(self._directory).resolve() == Path.cwd().resolve()
            and (not self._informer.useScratch() or self._host == socket.gethostname())
        )

    def _setDestination(self):
        """
        Determine the job's host and working directory from the QQInformer.

        Updates:
            - _host: hostname of the node where the job runs
            - _directory: absolute path to the working directory
        """
        destination = self._informer.getDestination()
        logger.debug(f"Destination: {destination}.")

        if destination:
            (self._host, self._directory) = destination
        else:
            self._host = None
            self._directory = None

    def _isQueued(self) -> bool:
        """Check if the job is queued, booting, held, or waiting."""
        return self._state in {
            RealState.QUEUED,
            RealState.BOOTING,
            RealState.HELD,
            RealState.WAITING,
        }

    def _isKilled(self) -> bool:
        """Check if the job has been killed."""
        return self._state == RealState.KILLED

    def isFinished(self) -> bool:
        """Check if the job has finished succesfully."""
        return self._state == RealState.FINISHED

    def _isFailed(self) -> bool:
        """Check if the job has failed."""
        return self._state == RealState.FAILED

    def _isRunning(self) -> bool:
        """Check if the job is currently running or suspended."""
        return self._state in {RealState.RUNNING, RealState.SUSPENDED}
