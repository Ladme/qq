# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import stat
import subprocess
import sys
from datetime import datetime
from pathlib import Path
import time
import readchar
from rich.console import Console
from rich.text import Text
from rich.live import Live

import click

from qq_lib.base import QQBatchInterface
from qq_lib.common import get_info_file
from qq_lib.error import QQError
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger
from qq_lib.pbs import QQPBS

logger = get_logger(__name__)
console = Console()

@click.command()
@click.option("-y", is_flag = True, help = "Assume yes.")
@click.option("--force", is_flag = True, help = "Try to kill any type of job. Assume yes.")
def kill(y: bool = False, force: bool = False):
    """
    Kill the qq job submitted from this directory. By default only kills queued and submitted jobs.
    """
    try:
        killer = QQKiller(QQPBS, Path("."))
        killer.printInfo()
        if force:
            killer.terminateForce()
        if not killer.isFinished() and (y or killer.askForConfirm()):
            killer.terminate()
        
        sys.exit(0)
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

        logger.debug(f"Loading QQInformer from '{self.info_file}'.")
        self.info = QQInformer.loadFromFile(self.info_file)

        self.state = self.info.getState()
        self.jobid = self.info.getJobId()

    def printInfo(self):
        logger.info(f"Found job '{self.jobid}' in '{self.state}' state.")

    def terminate(self):
        command = self.batch_system.translateKill(self.jobid)

        logger.debug(command)
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode == 0:
            self.info.setKilled(datetime.now())
            self.info.exportToFile(self.info_file)
            self._lockFile(self.info_file)
            logger.info(f"Killed job '{self.jobid}'.")
        else:
            raise QQError(f"Unable to kill job '{self.jobid}'.")

    def terminateForce(self):
        command = self.batch_system.translateKill(self.jobid)

        logger.debug(command)
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        logger.info("Attempting to forcefully kill the job.")

        if result.returncode == 0:
            if self.state == "running" or self.state == "queued":
                self.info.setKilled(datetime.now())
                self.info.exportToFile(self.info_file)
                self._lockFile(self.info_file)
            logger.info(f"Killed job '{self.jobid}'.")
        else:
            if self.state == "finished" or self.state == "failed":
                raise QQError(f"Job has already finished.")
            elif self.state == "killed":
                raise QQError(f"Job has already been killed.")
            else:
                raise QQError(f"Unable to kill job '{self.jobid}'.")
        
    def askForConfirm(self) -> bool:
        prompt = "   Do you want to kill the job? "
        text = Text("PROMPT", style="magenta") + Text(prompt, style = "default") + Text("[y/N]", style="bold default")

        with Live(text, refresh_per_second=10) as live:
            key = readchar.readkey().lower()

            # highlight the pressed key
            if key == "y":
                choice = Text("[", style="bold default") + Text("y", style="bold green") + Text("/N]", style="bold default")
            else:
                choice = Text("[y/", style="bold default") + Text("N", style="bold red") + Text("]", style="bold default")

            live.update(Text("PROMPT", style="magenta") + Text(prompt, style = "default") + choice)

        return key == "y"

    def isFinished(self) -> bool:
        if self.state == "finished" or self.state == "failed":
            raise QQError("Job has already finished.")
        
        if self.state == "killed":
            raise QQError("Job has already been killed.")
        
        return False


    def _lockFile(self, file_path: Path):
        """
        Removes write permissions for an info file so that the information about the kill is not overwritten.

        This is needed since when a job is killed in the batch system, it may take some time before it stops
        being executed, so it may potentially overwrite the info file.
        """
        current_mode = os.stat(file_path).st_mode
        new_mode = current_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)

        os.chmod(file_path, new_mode)
