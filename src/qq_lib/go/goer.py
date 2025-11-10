# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from time import sleep

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.core.navigator import Navigator

logger = get_logger(__name__)


class Goer(Navigator):
    """
    Provides utilities to open a shell in the working directory of a qq job.
    """

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

    def go(self) -> None:
        """
        Open a shell in the job's working directory on the main execution node.

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

        logger.info(f"Navigating to '{str(self._work_dir)}' on '{self._main_node}'.")
        self._batch_system.navigateToDestination(self._main_node, self._work_dir)

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
            self.update()
            self.ensureSuitable()
