# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import socket
from pathlib import Path
from time import sleep

from rich.console import Console

from qq_lib.core.constants import GOER_WAIT_TIME
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import QQInformer
from qq_lib.info.presenter import QQPresenter
from qq_lib.properties.states import RealState

logger = get_logger(__name__)


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

    def printInfo(self, console: Console):
        """
        Display the current job information in a formatted Rich panel.

        Args:
            console (Console): Rich Console instance used to render output.
        """
        presenter = QQPresenter(self._informer)
        panel = presenter.createJobStatusPanel(console)
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
        elif self.isKilled():
            if not self.hasDestination():
                raise QQError(
                    "Job has been killed and no working directory is available."
                )
            logger.warning(
                "Job has been killed: working directory may no longer exist."
            )
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

    def hasDestination(self) -> bool:
        """
        Check that the job has an assigned host and working directory.

        Returns:
            bool: True if the job has both a host and a working directory,
            False otherwise.
        """
        return self._directory and self._host

    def isJob(self, job_id: str) -> bool:
        """
        Determine whether this goer corresponds to the specified job ID.

        Args:
            job_id (str): The job ID to compare against (e.g., "12345" or "12345.cluster.domain").

        Returns:
            bool: True if both job IDs refer to the same job (same numeric/job part),
                False otherwise.
        """
        return self._informer.isJob(job_id)

    def _navigate(self):
        """
        Navigate to the job's working directory using batch system-specific commands.

        Raises:
            QQError: If host or directory are undefined, or if navigation fails.
        """
        if not self.hasDestination():
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
              a) either an input_dir was used to run the job, or
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
            and self._directory.resolve() == Path.cwd().resolve()
            and (not self._informer.useScratch() or self._host == socket.gethostname())
        )

    def _setDestination(self):
        """
        Get the job's host and working directory from the QQInformer.

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

    def isKilled(self) -> bool:
        """Check if the job has been killed."""
        return self._state == RealState.KILLED or (
            self._state == RealState.EXITING
            and self._informer.info.job_exit_code is None
        )

    def isFinished(self) -> bool:
        """Check if the job has finished succesfully."""
        return self._state == RealState.FINISHED

    def _isFailed(self) -> bool:
        """Check if the job has failed."""
        return self._state == RealState.FAILED

    def _isRunning(self) -> bool:
        """Check if the job is currently running or suspended."""
        return self._state in {
            RealState.RUNNING,
            RealState.SUSPENDED,
            RealState.EXITING,
        }
