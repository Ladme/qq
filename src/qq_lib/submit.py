# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import socket
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import click

from qq_lib.base import QQBatchInterface
from qq_lib.common import QQ_SUFFIXES
from qq_lib.env_vars import GUARD, INFO_FILE, JOBDIR, STDERR_FILE, STDOUT_FILE
from qq_lib.error import QQError
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger
from qq_lib.pbs import QQPBS
from qq_lib.resources import QQResources

logger = get_logger("qq submit", True)


@click.command()
@click.argument("queue", type=str)
@click.argument("script", type=str)
@click.option("--ncpus", type=int)
@click.option("--vnode", type=str)
def submit(
    queue: str,
    script: str,
    ncpus: int | None = None,
    vnode: str | None = None,
):
    """
    Submit a script to the batch system.
    """
    try:
        submitter = QQSubmitter(QQPBS, queue, script)
        submitter.guard()
        submitter.resources.setNCPUs(ncpus)
        submitter.resources.setVnode(vnode)

        sys.exit(submitter.submit())
    except QQError as e:
        logger.error(e)
        sys.exit(1)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(1)


class QQSubmitter:
    def __init__(
        self,
        batch_system: type[QQBatchInterface],
        queue: str,
        script: str,
    ):
        self.batch_system = batch_system
        self.queue = queue
        self.script = Path(script)
        self.resources = QQResources()

        if not self.script.is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

        if not self._hasValidShebang(self.script):
            raise QQError(
                f"Script '{self.script}' has an invalid shebang. The first line of the script should be '#!/usr/bin/env -S qq run'."
            )

    def submit(self) -> int:
        self._setQQEnv()
        self._setJobDir()
        self._setOutputFiles()

        command = self.batch_system.translateSubmit(
            self.resources, self.queue, str(self.script)
        )
        logger.debug(command)

        try:
            result = subprocess.run(
                ["bash"], input=command, text=True, check=False, capture_output=True
            )
        except Exception as e:
            raise QQError(f"Failed to submit script '{self.script}': {e}") from e

        if result.returncode == 0:
            # submission successful
            jobid = result.stdout.strip()
            logger.info(f"Submitted the job as '{jobid}'")

            info = QQInformer()
            info.setSubmitted(
                str(self.script),
                "standard",
                socket.gethostname(),
                self.job_dir,
                self.resources,
                None,
                datetime.now(),
                jobid,
            )
            info.exportToFile(self.info_file)
        else:
            raise QQError(f"Failed to submit script '{self.script}': {result.stderr}")

        return result.returncode

    def guard(self):
        """
        Guards against multiple submissions from the same directory.
        """
        if self._qqFilesPresent():
            raise QQError(
                "Detected qq runtime files. Multiple submissions from the same directory are not allowed. "
                "To clear the qq runtime files, run 'qq clear'."
            )

    def _qqFilesPresent(self) -> bool:
        current_dir = Path(".")
        for file in current_dir.iterdir():
            if file.is_file() and file.suffix in QQ_SUFFIXES:
                return True
        return False

    def _setJobDir(self):
        self.job_dir = Path(os.path.abspath(os.getcwd()))
        os.environ[JOBDIR] = str(self.job_dir)

    def _setQQEnv(self):
        os.environ[GUARD] = "true"

    def _setOutputFiles(self):
        os.environ[STDOUT_FILE] = str(self.script.with_suffix(".stdout"))
        os.environ[STDERR_FILE] = str(self.script.with_suffix(".stderr"))
        self.info_file = self.script.with_suffix(".qqinfo").resolve()
        os.environ[INFO_FILE] = str(self.info_file)

    def _hasValidShebang(self, script: Path) -> bool:
        with open(script) as file:
            first_line = file.readline()
            return first_line.startswith("#!") and first_line.strip().endswith("qq run")
