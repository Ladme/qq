# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import stat
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console

from qq_lib.common import get_info_file, yes_or_no_prompt
from qq_lib.error import QQError
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger
from qq_lib.states import QQState

logger = get_logger(__name__)
console = Console()


@click.command()
@click.option("-y", is_flag=True, help="Assume yes.")
@click.option("--force", is_flag=True, help="Try to kill any type of job. Assume yes.")
def kill(y: bool = False, force: bool = False):
    """
    Kill the qq job submitted from this directory. By default only kills queued and submitted jobs.
    """
    try:
        killer = QQKiller(Path(), force)
        killer.printInfo()
        if killer.shouldTerminate():
            if force or y or killer.askForConfirm():
                killer.terminate(force)
                if killer.shouldUpdateInfoFile(force):
                    killer.updateInfoFile()
                logger.info(f"Killed the job '{killer.jobid}'.")
        else:
            raise QQError(
                "Job is already completed or terminated. Try using the '--force' option."
            )
        print()
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        print()
        sys.exit(1)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(1)


class QQKiller:
    def __init__(self, current_dir: Path, forced: bool):
        self.info_file = get_info_file(current_dir)
        self.info = QQInformer.loadFromFile(self.info_file)
        self.batch_system = self.info.batch_system

        self.state = self.info.getRealState()
        self.jobid = self.info.getJobId()

        self.forced = forced

    def printInfo(self):
        panel = self.info.getJobStatusPanel()
        console.print(panel)

    def askForConfirm(self) -> bool:
        return yes_or_no_prompt("Do you want to kill the job?")

    def shouldTerminate(self) -> bool:
        return self.forced or (not self._isFinished() and not self._isKilled())

    def terminate(self, force: bool):
        if force:
            command = self.batch_system.translateKillForce(self.jobid)
        else:
            command = self.batch_system.translateKill(self.jobid)

        logger.debug(command)
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode != 0:
            raise QQError(f"Could not kill the job: {result.stderr.strip()}.")

    def shouldUpdateInfoFile(self, force: bool) -> bool:
        """
        Determine whether the qq kill process should update the info file.

        This method evaluates whether qq kill should log the information about
        the job's termination should be into the qq info file. This is necessary
        in cases where qq run may not be able to log the job information, such as when the
        job is forcefully killed or has not yet started running.

        Parameters:
            force (bool):
                If True, indicates that the job was forcefully killed and the info
                file must be updated regardless of the job's queued state.

        Returns:
            bool:
                True if the info file should be updated by the qq kill process,
                False otherwise.

        Conditions for updating the info file (all points must be true):
            - The job is forcefully killed (`force=True`)
                OR the job is queued/booting/suspended.
            - The job is not finished.
            - The job has not already been killed.
            - The job is not in an unknown or inconsistent state.
        """
        return (
            (force or self._isQueued() or self._isBooting() or self._isSuspended())
            and not self._isFinished()
            and not self._isKilled()
            and not self._isUnknownInconsistent()
        )

    def updateInfoFile(self):
        self.info.setKilled(datetime.now())
        self.info.exportToFile(self.info_file)
        # strictly speaking, we only need to lock the info file
        # when dealing with a booting job but this does not actually hurt anything
        self._lockFile(self.info_file)

    def _isBooting(self) -> bool:
        return self.state == QQState.BOOTING
    
    def _isSuspended(self) -> bool:
        return self.state == QQState.SUSPENDED

    def _isQueued(self) -> bool:
        return self.state in [QQState.QUEUED, QQState.HELD, QQState.WAITING]

    def _isKilled(self) -> bool:
        return self.state == QQState.KILLED

    def _isFinished(self) -> bool:
        return self.state in [QQState.FINISHED, QQState.FAILED]

    def _isUnknownInconsistent(self) -> bool:
        return self.state in [QQState.UNKNOWN, QQState.IN_AN_INCONSISTENT_STATE]

    def _lockFile(self, file_path: Path):
        """
        Removes write permissions for an info file so that the information about the kill is not overwritten.
        """
        current_mode = Path.stat(file_path).st_mode
        new_mode = current_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)

        Path.chmod(file_path, new_mode)
