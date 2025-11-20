# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import stat
from datetime import datetime
from pathlib import Path

from qq_lib.core.error import QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.core.operator import Operator
from qq_lib.properties.states import RealState

logger = get_logger(__name__)


class Killer(Operator):
    """
    Class to manage the termination of a qq job.
    """

    def ensureSuitable(self) -> None:
        """
        Verify that the job is in a state where it can be terminated.

        Raises:
            QQNotSuitableError: If the job has already finished successfully,
                                has already been killed, or is currently exiting.
        """
        if self._isCompleted():
            raise QQNotSuitableError(
                "Job cannot be terminated. Job is already completed."
            )

        if self._isKilled():
            raise QQNotSuitableError(
                "Job cannot be terminated. Job has already been killed."
            )

        if self._isExiting():
            raise QQNotSuitableError(
                "Job cannot be terminated. Job is in an exiting state."
            )

    def kill(self, force: bool = False) -> str:
        """
        Execute the kill command for the job using the batch system.

        Returns:
            str: The identifier of the terminated job.

        Raises:
            QQError: If the kill command fails.
        """
        # has to be performed before actually killing the job
        should_update = self._shouldUpdateInfoFile(force)

        if force:
            self._batch_system.jobKillForce(self._informer.info.job_id)
        else:
            self._batch_system.jobKill(self._informer.info.job_id)

        if should_update:
            self._updateInfoFile()

        return self._informer.info.job_id

    def _shouldUpdateInfoFile(self, force: bool) -> bool:
        """
        Determine whether the killer itself should log that
        the job has been killed into the info file.

        Args:
            force (bool): The job is being killed forcibly.

        Returns:
            bool:
                True if the info file should be updated by the qq kill process,
                False otherwise.
        """

        return (
            (force or self._isQueued() or self._isSuspended())
            and not self._isCompleted()
            and not self._isKilled()
            and not self._isUnknownInconsistent()
        )

    def _updateInfoFile(self) -> None:
        """
        Mark the job as killed in the info file and lock it to prevent overwriting.
        """
        self._informer.setKilled(datetime.now())
        self._informer.toFile(self._info_file)
        # strictly speaking, we only need to lock the info file
        # when dealing with a booting job but doing it for the other jobs
        # which state is managed by `qq kill` does not hurt anything
        self._lockFile(self._info_file)

    def _isSuspended(self) -> bool:
        """Check if the job is currently suspended."""
        return self._state == RealState.SUSPENDED

    def _isQueued(self) -> bool:
        """Check if the job is queued, held, waiting, or booting."""
        return self._state in {
            RealState.QUEUED,
            RealState.HELD,
            RealState.WAITING,
            RealState.BOOTING,
        }

    def _isKilled(self) -> bool:
        """Check if the job has already been killed."""
        return self._state == RealState.KILLED or (
            self._state == RealState.EXITING
            and self._informer.info.job_exit_code is None
        )

    def _isCompleted(self) -> bool:
        """Check if the job has finished or failed."""
        return self._state in {RealState.FINISHED, RealState.FAILED}

    def _isExiting(self) -> bool:
        """Check if the job is currently exiting."""
        return self._state == RealState.EXITING

    def _isUnknownInconsistent(self) -> bool:
        """Check if the job is in an unknown or inconsistent state."""
        return self._state in {RealState.UNKNOWN, RealState.IN_AN_INCONSISTENT_STATE}

    def _lockFile(self, file_path: Path) -> None:
        """
        Remove write permissions for an info file to prevent overwriting
        information about the killed job.
        """
        current_mode = Path.stat(file_path).st_mode
        new_mode = current_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)

        Path.chmod(file_path, new_mode)
