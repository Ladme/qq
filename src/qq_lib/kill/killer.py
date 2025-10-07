# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import stat
from datetime import datetime
from pathlib import Path

from rich.console import Console

from qq_lib.core.common import yes_or_no_prompt
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import QQInformer
from qq_lib.info.presenter import QQPresenter
from qq_lib.properties.states import RealState

logger = get_logger(__name__)
console = Console()


class QQKiller:
    """
    Class to manage the termination of a qq job.
    """

    def __init__(self, info_file: Path, forced: bool):
        """
        Initialize a QQKiller instance.

        Args:
            info_file (Path): Path to the qq info file.
            forced (bool): Whether to forcefully terminate the job.
        """
        self._info_file = info_file
        self._informer = QQInformer.fromFile(self._info_file)
        self._batch_system = self._informer.batch_system
        self._state = self._informer.getRealState()
        self._forced = forced

    def getJobId(self) -> str:
        """
        Get the job ID of the job to kill.
        """
        return self._informer.info.job_id

    def printInfo(self):
        """
        Display the current job status using a formatted panel.
        """
        presenter = QQPresenter(self._informer)
        panel = presenter.createJobStatusPanel(console)
        console.print(panel)

    def askForConfirm(self) -> bool:
        """
        Prompt the user for confirmation to kill the job.

        Returns:
            bool: True if user confirms, False otherwise.
        """
        return yes_or_no_prompt("Do you want to kill the job?")

    def shouldTerminate(self) -> bool:
        """
        Determine if the job should be terminated based on its current state
        and whether forced termination is requested.

        Returns:
            bool: True if termination should proceed, False otherwise.
        """
        return self._forced or (
            not self._isFinished() and not self._isKilled() and not self._isExiting()
        )

    def terminate(self):
        """
        Execute the kill command for the job using the batch system.

        Raises:
            QQError: If the kill command fails.
        """
        if self._forced:
            self._batch_system.jobKillForce(self._informer.info.job_id)
        else:
            self._batch_system.jobKill(self._informer.info.job_id)

    def shouldUpdateInfoFile(self) -> bool:
        """
        Determine whether the qq kill process should update the info file.

        This method evaluates whether qq kill should log the information about
        the job's termination should be into the qq info file. This is necessary
        in cases where qq run may not be able to log the job information, such as when the
        job is forcibly killed or has not yet started running.

        Returns:
            bool:
                True if the info file should be updated by the qq kill process,
                False otherwise.

        Conditions for updating the info file (all points must be true):
            - The job is forcibly killed (`self.forced=True`)
                OR the job is queued/booting/suspended.
            - The job is not finished.
            - The job has not already been killed.
            - The job is not in an unknown or inconsistent state.
        """
        return (
            (
                self._forced
                or self._isQueued()
                or self._isBooting()
                or self._isSuspended()
            )
            and not self._isFinished()
            and not self._isKilled()
            and not self._isUnknownInconsistent()
        )

    def updateInfoFile(self):
        """
        Mark the job as killed in the info file and lock it to prevent overwriting.
        """
        self._informer.setKilled(datetime.now())
        self._informer.toFile(self._info_file)
        # strictly speaking, we only need to lock the info file
        # when dealing with a booting job but doing it for the other jobs
        # which state is managed by `qq kill` does not hurt anything
        self._lockFile(self._info_file)

    def _isBooting(self) -> bool:
        """Check if the job is currently booting."""
        return self._state == RealState.BOOTING

    def _isSuspended(self) -> bool:
        """Check if the job is currently suspended."""
        return self._state == RealState.SUSPENDED

    def _isQueued(self) -> bool:
        """Check if the job is queued, held, or waiting."""
        return self._state in {RealState.QUEUED, RealState.HELD, RealState.WAITING}

    def _isKilled(self) -> bool:
        """Check if the job has already been killed."""
        return self._state == RealState.KILLED

    def _isFinished(self) -> bool:
        """Check if the job has finished or failed."""
        return self._state in {RealState.FINISHED, RealState.FAILED}

    def _isExiting(self) -> bool:
        """Check if the job is currently exiting."""
        return self._state == RealState.EXITING

    def _isUnknownInconsistent(self) -> bool:
        """Check if the job is in an unknown or inconsistent state."""
        return self._state in {RealState.UNKNOWN, RealState.IN_AN_INCONSISTENT_STATE}

    def _lockFile(self, file_path: Path):
        """
        Remove write permissions for an info file to prevent overwriting
        information about the killed job.
        """
        current_mode = Path.stat(file_path).st_mode
        new_mode = current_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)

        Path.chmod(file_path, new_mode)
