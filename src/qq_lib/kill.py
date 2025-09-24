# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Module for terminating qq jobs submitted from the current directory.

Read the documentation of the `kill` function for more details.
"""

import stat
import sys
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console

from qq_lib.common import get_info_file, yes_or_no_prompt
from qq_lib.error import QQError
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger
from qq_lib.states import RealState

logger = get_logger(__name__)
console = Console()


@click.command(
    help="""Terminate the qq job.

Unless the `-y` or `--force` flag is used, `qq kill` always
asks for confirmation before killing a job.

By default (without --force), `qq kill` will only attempt to kill jobs
that are queued, held, booting, or running but not yet finished or already killed.

When the --force flag is used, `qq kill` will attempt to terminate any job
regardless of its state, including jobs that are, according to the qq,
already finished or killed. This can be used to remove lingering (stuck) jobs."""
)
@click.option(
    "-y", "--yes", is_flag=True, help="Terminate the job without confirmation."
)
@click.option(
    "--force", is_flag=True, help="Kill the job forcibly and without confirmation."
)
def kill(yes: bool = False, force: bool = False):
    """
    Terminate a qq job submitted from the current directory.

    Unless the `-y` or `--force` flag is used, `qq kill` always
    asks for confirmation before killing a job.

    By default (without --force), `qq kill` will only attempt to kill jobs
    that are queued, held, booting, or running but not yet finished or already killed.

    When the --force flag is used, `qq kill` will attempt to terminate any job
    regardless of its state, including jobs that are, according to the qq,
    already finished or killed. This can be used to remove lingering (stuck) jobs.

    Details
        Killing a job sets its state to "killed". This is handled either by `qq kill` or
        `qq run`, depending on job type and whether the `--force` flag was used:

        - Forced kills: `qq kill` updates the qq info file to mark the
            job as killed, because `qq run` may not have time to do so.
            The info file is subsequently locked to avoid overwriting.

        - Jobs that have not yet started: `qq run` does not exist yet, so
            `qq kill` is responsible for marking the job as killed.

        - Jobs that are booting: `qq run` does exist for booting jobs, but
            it is unreliable at this stage. PBS's `qdel` may also silently fail for
            booting jobs. `qq kill` is thus responsible for setting the job state
            and locking the info file (which then forces `qq run` to terminate
            even if the batch system fails to kill it).

        - Normal (non-forced) termination: `qq run` is responsible for
            updating the job state in the info file once the job is terminated.
    """
    try:
        killer = QQKiller(Path(), force)
        killer.printInfo()

        if killer.shouldTerminate():
            if force or yes or killer.askForConfirm():
                should_update = killer.shouldUpdateInfoFile()
                killer.terminate()
                if should_update: 
                    killer.updateInfoFile()
                logger.info(f"Killed the job '{killer.getJobId()}'.")
        else:
            raise QQError(
                "Job is already completed or terminated. Try using the '--force' option."
            )
        print()
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        print()
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


class QQKiller:
    """
    Class to manage the termination of a qq job.
    """

    def __init__(self, current_dir: Path, forced: bool):
        """
        Initialize a QQKiller instance.

        Args:
            current_dir (Path): Directory containing the qq job info file.
            forced (bool): Whether to forcefully terminate the job.
        """
        self._info_file = get_info_file(current_dir)
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
        panel = self._informer.createJobStatusPanel(console)
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
        return self._forced or (not self._isFinished() and not self._isKilled())

    def terminate(self):
        """
        Execute the kill command for the job using the batch system.

        Raises:
            QQError: If the kill command fails.
        """
        if self._forced:
            result = self._batch_system.jobKillForce(self._informer.info.job_id)
        else:
            result = self._batch_system.jobKill(self._informer.info.job_id)

        if result.exit_code != 0:
            raise QQError(f"Could not kill the job: {result.error_message}.")

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
        return self._state in [RealState.QUEUED, RealState.HELD, RealState.WAITING]

    def _isKilled(self) -> bool:
        """Check if the job has already been killed."""
        return self._state == RealState.KILLED

    def _isFinished(self) -> bool:
        """Check if the job has finished or failed."""
        return self._state in [RealState.FINISHED, RealState.FAILED]

    def _isUnknownInconsistent(self) -> bool:
        """Check if the job is in an unknown or inconsistent state."""
        return self._state in [RealState.UNKNOWN, RealState.IN_AN_INCONSISTENT_STATE]

    def _lockFile(self, file_path: Path):
        """
        Remove write permissions for an info file to prevent overwriting
        information about the killed job.
        """
        current_mode = Path.stat(file_path).st_mode
        new_mode = current_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)

        Path.chmod(file_path, new_mode)
