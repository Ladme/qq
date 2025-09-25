# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
This module manages submission of qq jobs using the QQSubmitter class.
"""

import getpass
import os
import socket
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import click

import qq_lib
from qq_lib.batch import QQBatchInterface, QQBatchMeta
from qq_lib.clear import QQClearer
from qq_lib.common import yes_or_no_prompt
from qq_lib.constants import (
    GUARD,
    INFO_FILE,
    QQ_INFO_SUFFIX,
    QQ_SUFFIXES,
    STDERR_SUFFIX,
    STDOUT_SUFFIX,
)
from qq_lib.error import QQError
from qq_lib.info import QQInfo, QQInformer
from qq_lib.logger import get_logger
from qq_lib.resources import QQResources
from qq_lib.states import NaiveState

logger = get_logger(__name__)


@click.command(short_help="Submit a qq job to the batch system.")
@click.argument("queue", type=str)
@click.argument("script", type=str)
@click.option("--ncpus", type=int, default=None)
@click.option("--vnode", type=str, default=None)
@click.option("--walltime", type=str, default=None)
@click.option("--work-dir", type=str, default="scratch_local")
@click.option("--work-size", type=str, default=None)
@click.option("--batch-system", type=str, default="PBS")
def submit(queue, script, **kwargs):
    """
    Submit a qq job to a batch system from the command line.

    Note that the submitted script must be located in the same directory from which 'qq submit' is invoked.
    """
    try:
        BatchSystem = QQBatchMeta.fromStr(kwargs["batch_system"])
        del kwargs["batch_system"]
        resources = QQResources(**kwargs)
        submitter = QQSubmitter(BatchSystem, queue, Path(script), resources)
        if not submitter.isShared(Path()):
            raise QQError(
                "Submitting qq jobs is only possible from a shared filesystem."
            )
        submitter.guardOrClear()
        submitter.submit()
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


class QQSubmitter:
    """
    Class to submit jobs to a batch system.

    Responsibilities:
        - Validate that the script exists and has a proper shebang.
        - Guard against multiple submissions from the same directory.
        - Set environment variables required for `qq run`.
        - Create QQInfo files for tracking job state and metadata.
    """

    def __init__(
        self,
        batch_system: type[QQBatchInterface],
        queue: str,
        script: Path,
        resources: QQResources,
    ):
        """
        Initialize a QQSubmitter instance.

        Args:
            batch_system: A class implementing QQBatchInterface.
            queue: Name of the batch system queue to use.
            script: Path to the script to submit.
            resources: QQResources instance with job requirements.

        Raises:
            QQError: If the script does not exist, is not in the current directory,
                     or has an invalid shebang.
        """

        self._batch_system = batch_system
        self._queue = queue
        self._script = script
        self._script_name = script.name  # strip any potential absolute path
        self._info_file = self._script.with_suffix(QQ_INFO_SUFFIX).resolve()
        self._resources = resources

        # script must exist
        if not self._script.is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

        # script must exist in the current directory
        if self._script.parent.resolve() != Path.cwd():
            raise QQError(f"Script '{script}' is not in the submission directory.")

        if not self._hasValidShebang(self._script):
            raise QQError(
                f"Script '{self._script}' has an invalid shebang. The first line of the script should be '#!/usr/bin/env -S qq run'."
            )

    def submit(self) -> str:
        """
        Submit the script to the batch system.

        Sets required environment variables, calls the batch system's
        job submission mechanism, and creates a QQInfo file with job metadata.

        Returns:
            The job ID of the submitted job.

        Raises:
            QQError: If job submission fails.
        """
        # setting the basic environment variables for communicating with `qq run`
        self._setEnvVars()

        # submit the job
        result = self._batch_system.jobSubmit(
            self._resources, self._queue, self._script
        )

        if result.exit_code == 0:
            # submission succesful
            job_id = result.success_message
            logger.info(f"Job '{job_id}' submitted successfully.")
            informer = QQInformer(
                QQInfo(
                    batch_system=self._batch_system,
                    qq_version=qq_lib.__version__,
                    username=getpass.getuser(),
                    job_id=job_id,
                    job_name=self._script_name,
                    script_name=self._script_name,
                    job_type="standard",
                    input_machine=socket.gethostname(),
                    job_dir=Path.cwd(),
                    job_state=NaiveState.QUEUED,
                    submission_time=datetime.now(),
                    stdout_file=str(Path(self._script_name).with_suffix(STDOUT_SUFFIX)),
                    stderr_file=str(Path(self._script_name).with_suffix(STDERR_SUFFIX)),
                    resources=self._resources,
                )
            )

            informer.toFile(self._info_file)
            return job_id
        # submission failed
        raise QQError(
            f"Failed to submit script '{self._script}': {result.error_message}."
        )

    def isShared(self, directory: Path) -> bool:
        """
        Checks that the specified directory is on a shared filesystem.
        """
        # df -l exits with zero if the filesystem is local; otherwise it exits with a non-zero code
        result = subprocess.run(
            ["df", "-l", directory],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        return result.returncode != 0

    def guardOrClear(self):
        """
        Prevent multiple submissions from the same directory.

        If no qq runtime files are present, return immediately.
        If invalid qq runtime files are detected, warn the user and prompt whether to clear them.
        - If the user agrees, clear the files and continue.
        - If the user declines, raise QQError.

        If the files belong to an active or successfully finished job, always raise QQError.

        Raises:
            QQError: If QQ runtime files from an active/successful run are detected,
                    or if invalid QQ files are present and the user chooses not to clear them.
        """
        if not self._qqFilesPresent():
            return  # no qq files present, all good

        clearer = QQClearer(Path.cwd())
        if clearer.shouldClear(force=False):
            logger.warning(
                "Detected qq runtime files from an invalid run. Submission suspended."
            )
            if yes_or_no_prompt(
                "Do you want to remove these files and submit the job?"
            ):
                files = clearer.getQQFiles()
                clearer.clearFiles(files, False)
            else:
                raise QQError("Submission aborted.")
        else:
            raise QQError(
                "Detected qq runtime files from an active or successful run. Submission aborted!"
            )

    def _qqFilesPresent(self) -> bool:
        """
        Check for presence of qq runtime files in the current directory.

        Returns:
            True if files with QQ_SUFFIXES are present, False otherwise.
        """
        current_dir = Path()
        for file in current_dir.iterdir():
            if file.is_file() and file.suffix in QQ_SUFFIXES:
                return True
        return False

    def _setEnvVars(self):
        """
        Set environment variables required for qq runtime.
        """
        # this indicates that the job is running in a qq environment
        os.environ[GUARD] = "true"

        # this contains a path to the qq info file
        os.environ[INFO_FILE] = str(self._info_file)

    def _hasValidShebang(self, script: Path) -> bool:
        """
        Verify that the script has a valid shebang for qq run.

        Args:
            script: Path to the script file.

        Returns:
            True if the first line starts with '#!' and ends with 'qq run'.
        """
        with Path.open(script) as file:
            first_line = file.readline()
            return first_line.startswith("#!") and first_line.strip().endswith("qq run")
