# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import subprocess
import sys
from pathlib import Path

import click

from qq_lib.base import QQBatchInterface
from qq_lib.env_vars import GUARD, JOBDIR, STDERR_FILE, STDOUT_FILE
from qq_lib.error import QQError
from qq_lib.logger import get_logger
from qq_lib.pbs import QQPBS
from qq_lib.properties import QQProperties

logger = get_logger("qq submit")


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
        submitter.properties.setNCPUs(ncpus)
        submitter.properties.setVnode(vnode)

        sys.exit(submitter.submit())
    except Exception as e:
        logger.error(e)
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
        self.properties = QQProperties()
        self._setOutputFiles(script)

        if not self.script.is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

        if not self._hasValidShebang(self.script):
            raise QQError(
                f"Script '{self.script}' has an invalid shebang. The first line of the script should be '#!/usr/bin/env -S qq run'."
            )

    def submit(self) -> int:
        self._setJobDir()

        command = self.batch_system.translateSubmit(
            self.properties, self.queue, str(self.script)
        )

        try:
            result = subprocess.run(["bash"], input=command, text=True, check=False)

            return result.returncode
        except Exception as e:
            raise QQError(f"Failed to submit script '{self.script}': {e}") from e

    def _setJobDir(self):
        os.environ[JOBDIR] = os.path.abspath(os.getcwd())

    def _setQQEnv(self):
        os.environ[GUARD] = "true"

    def _setOutputFiles(self, script: str):
        os.environ[STDOUT_FILE] = script + ".stdout"
        os.environ[STDERR_FILE] = script + ".stderr"

        logger.debug(f"STDOUT FILE: {os.environ.get(STDOUT_FILE)}")
        logger.debug(f"STDERR FILE: {os.environ.get(STDERR_FILE)}")

    def _hasValidShebang(self, script: Path) -> bool:
        with open(script) as file:
            first_line = file.readline()
            return first_line.startswith("#!") and first_line.strip().endswith("qq run")
