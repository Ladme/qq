# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import socket
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import click

from qq_lib.batch import QQBatchInterface
from qq_lib.constants import (
    GUARD,
    INFO_FILE,
)
from qq_lib.error import QQError
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger
from qq_lib.pbs import QQPBS
from qq_lib.resources import QQResources
from qq_lib.constants import QQ_INFO_SUFFIX, QQ_SUFFIXES, STDERR_SUFFIX, STDOUT_SUFFIX

logger = get_logger(__name__)


@click.command()
@click.argument("queue", type=str)
@click.argument("script", type=str)
@click.option("--ncpus", type=int, default=None)
@click.option("--vnode", type=str, default=None)
@click.option("--walltime", type=str, default=None)
@click.option("--workdir", type=str, default="scratch_local")
@click.option("--worksize", type=str, default=None)
def submit(
    queue: str,
    script: str,
    ncpus: int | None,
    vnode: str | None,
    walltime: str | None,
    workdir: str,
    worksize: str | None,
):
    """
    Submit a qq job to the batch system.
    """
    try:
        submitter = QQSubmitter(QQPBS, queue, script)
        submitter.guard()
        submitter.resources = QQResources(ncpus, vnode, walltime, workdir, worksize)
        sys.exit(submitter.submit())
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


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
        self.job_dir = Path.cwd().resolve()
        self.resources: QQResources | None = None
        self.info_file = self.script.with_suffix(QQ_INFO_SUFFIX).resolve()

        if not self.script.is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

        if not self._hasValidShebang(self.script):
            raise QQError(
                f"Script '{self.script}' has an invalid shebang. The first line of the script should be '#!/usr/bin/env -S qq run'."
            )

    def submit(self) -> int:
        # sanity check
        if not self.resources:
            raise QQError("Resources have not been set up.")

        # setting the basic environment variables for communicating with `qq run`
        self._setEnvVars()

        # get the submission command
        command = self.batch_system.translateSubmit(
            self.resources, self.queue, str(self.script)
        )
        logger.debug(command)

        # submit the script
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode == 0:
            # submission successful
            jobid = result.stdout.strip()
            logger.info(f"Job '{jobid}' submitted successfully.")

            # create a qq info file
            info = QQInformer(self.batch_system)
            info.setSubmitted(
                str(self.script),
                "standard",
                socket.gethostname(),
                self.job_dir,
                self.resources,
                None,
                datetime.now(),
                jobid,
                str(Path(self.script).with_suffix(STDOUT_SUFFIX)),
                str(Path(self.script).with_suffix(STDERR_SUFFIX)),
            )
            info.exportToFile(self.info_file)
        else:
            raise QQError(f"Failed to submit script '{self.script}': {result.stderr}.")

        return result.returncode

    def guard(self):
        """
        Guards against multiple submissions from the same directory.
        """
        if self._qqFilesPresent():
            raise QQError(
                "Detected qq runtime files. Multiple submissions from the same directory are not allowed. "
                "To clear the files, run 'qq clear'."
            )

    def _qqFilesPresent(self) -> bool:
        current_dir = Path()
        for file in current_dir.iterdir():
            if file.is_file() and file.suffix in QQ_SUFFIXES:
                return True
        return False

    def _setEnvVars(self):
        # this indicates that the job is running in a qq environment
        os.environ[GUARD] = "true"

        # this contains a path to the qq info file
        os.environ[INFO_FILE] = str(self.info_file)

    def _hasValidShebang(self, script: Path) -> bool:
        with Path.open(script) as file:
            first_line = file.readline()
            return first_line.startswith("#!") and first_line.strip().endswith("qq run")
