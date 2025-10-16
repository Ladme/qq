# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import socket
from pathlib import Path
from time import sleep

from rich.console import Console

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
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
        self._update()

    def printInfo(self, console: Console) -> None:
        """
        Display the current job information in a formatted Rich panel.

        Args:
            console (Console): Rich Console instance used to render output.
        """
        presenter = QQPresenter(self._informer)
        panel = presenter.createJobStatusPanel(console)
        console.print(panel)

    def hasDestination(self) -> bool:
        """
        Check that the job has an assigned host and working directory.

        Returns:
            bool: True if the job has both a host and a working directory,
            False otherwise.
        """
        return self._directory is not None and self._host is not None

    def ensureSuitable(self) -> None:
        """
        Verify that the job is in a state where its working directory can be visited.

        Raises:
            QQNotSuitableError: If the job has already finished / is finishing successfully
                                or has been killed without creating a working directory.
        """
        if self._isFinished():
            raise QQNotSuitableError(
                "Job has finished and was synchronized: working directory no longer exists."
            )

        if self._isExitingSuccessfully():
            raise QQNotSuitableError(
                "Job is finishing successfully: working directory no longer exists."
            )

        if self._isKilled() and not self.hasDestination():
            raise QQNotSuitableError(
                "Job has been killed and no working directory has been created."
            )

    def matchesJob(self, job_id: str) -> bool:
        """
        Determine whether this goer corresponds to the specified job ID.

        Args:
            job_id (str): The job ID to compare against (e.g., "12345" or "12345.cluster.domain").

        Returns:
            bool: True if both job IDs refer to the same job (same numeric/job part),
                False otherwise.
        """
        return self._informer.matchesJob(job_id)

    def go(self) -> None:
        """
        Navigate to the job's working directory on the main execution node.

        Raises:
            QQError: If the working directory or main node is not set and navigation
                    cannot proceed.

        Notes:
            - This method may block while waiting for a queued job to start.
        """
        if self._isInWorkDir():
            logger.info("You are already in the working directory.")
            return

        if self._isKilled():
            logger.warning(
                "Job has been killed: working directory may no longer exist."
            )

        elif self._isFailed():
            logger.warning(
                "Job has completed with an error code: working directory may no longer exist."
            )

        elif self._isUnknownInconsistent():
            logger.warning("Job is in an unknown, unrecognized, or inconsistent state.")

        elif self._isQueued():
            logger.warning(
                f"Job is {str(self._state)}: working directory does not yet exist. Will retry every {CFG.goer.wait_time} seconds."
            )

            # keep retrying until the job stops being queued
            self._waitQueued()
            if self._isInWorkDir():
                logger.info("You are already in the working directory.")
                return

        if not self.hasDestination():
            raise QQError(
                "Host ('main_node') or working directory ('work_dir') are not defined."
            )

        logger.info(f"Navigating to '{str(self._directory)}' on '{self._host}'.")
        self._batch_system.navigateToDestination(self._host, self._directory)

    def _update(self) -> None:
        """
        Refresh internal state of the goer from the qq info file.
        """
        self._informer = QQInformer.fromFile(self._info_file)
        self._batch_system = self._informer.info.batch_system
        self._state = self._informer.getRealState()
        self._setDestination()

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

    def _waitQueued(self):
        """
        Wait until the job is no longer in the queued state.

        Raises:
            QQNotSuitableError: If at any point the job is found to be finished
                                or killed without a working directory.

        Note:
            This is a blocking method and will continue looping until the job
            leaves the queued state or an exception is raised.
        """
        while self._isQueued():
            sleep(CFG.goer.wait_time)
            self._update()
            self.ensureSuitable()

    def _setDestination(self) -> None:
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

    def _isKilled(self) -> bool:
        """Check if the job has been or is being killed."""
        return self._state == RealState.KILLED or (
            self._state == RealState.EXITING
            and self._informer.info.job_exit_code is None
        )

    def _isFinished(self) -> bool:
        """Check if the job has finished succesfully."""
        return self._state == RealState.FINISHED

    def _isFailed(self) -> bool:
        """Check if the job has failed."""
        return self._state == RealState.FAILED

    def _isUnknownInconsistent(self) -> bool:
        """Check if the job is in an unknown or inconsistent state."""
        return self._state in {RealState.UNKNOWN, RealState.IN_AN_INCONSISTENT_STATE}

    def _isExitingSuccessfully(self) -> bool:
        """
        Check whether the job is currently successfully exiting.
        """
        return (
            self._state == RealState.EXITING and self._informer.info.job_exit_code == 0
        )
