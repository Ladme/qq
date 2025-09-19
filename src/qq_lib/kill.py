# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import stat
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import click

from qq_lib.base import QQBatchInterface
from qq_lib.common import get_info_file
from qq_lib.error import QQError
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger
from qq_lib.pbs import QQPBS

logger = get_logger("qq kill", True)


@click.command()
@click.option(
    "--force",
    is_flag=True,
    help="Forcefully terminate the job.",
)
def kill(force: bool = False):
    """
    Kill the qq job submitted from this directory.
    """
    try:
        killer = QQKiller(QQPBS, Path("."))
        if force:
            killer.terminateForce()
        else:
            killer.terminate()
    except QQError as e:
        logger.error(e)
        sys.exit(1)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(1)


class QQKiller:
    def __init__(self, batch_system: type[QQBatchInterface], current_dir: Path):
        self.batch_system = batch_system
        self.info_file = get_info_file(current_dir)

        logger.debug(f"Loading QQInformer from '{self.info_file}'")
        self.info = QQInformer.loadFromFile(self.info_file)

        self.state = self.info.getState()
        self.jobid = self.info.getJobId()

    def terminate(self):
        if self.state == "finished":
            raise QQError("Job is finished and synchronized (nothing to kill)")
        elif self.state == "failed":
            raise QQError("Job has failed executing (nothing to kill)")
        elif self.state == "killed":
            raise QQError("Job has been killed (nothing else to kill)")
        elif self.state == "running" or self.state == "queued":
            pass
        else:
            logger.warning(
                "Job is in an unknown, unrecognized, or inconsistent state"
            )

        command = self.batch_system.translateKill(self.jobid)

        logger.debug(command)
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode == 0:
            self.info.setKilled(datetime.now())
            self.info.exportToFile(self.info_file)
            self._lockFile(self.info_file)
            logger.info(f"Killed job '{self.jobid}'")
        else:
            raise QQError(f"Unable to kill job '{self.jobid}': {result.stderr.strip()}")

    def terminateForce(self):
        command = self.batch_system.translateKill(self.jobid)

        logger.debug(command)
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode == 0:
            if self.state == "running" or self.state == "queued":
                self.info.setKilled(datetime.now())
                self.info.exportToFile(self.info_file)
                self._lockFile(self.info_file)
            logger.info(f"Killed job '{self.jobid}' forcefully")
        else:
            raise QQError(
                f"Unable to force-kill job '{self.jobid}': {result.stderr.strip()}"
            )

    def _lockFile(self, file_path: Path):
        """
        Removes write permissions for an info file so that the information about the kill is not overwritten.

        This is needed since when a job is killed in the batch system, it may take some time before it stops
        being executed, so it may potentially overwrite the info file.
        """
        current_mode = os.stat(file_path).st_mode
        new_mode = current_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)

        os.chmod(file_path, new_mode)
