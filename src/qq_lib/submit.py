# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
This module manages submission of qq jobs using the QQSubmitter class.
"""

import getpass
import os
import socket
import sys
from datetime import datetime
from pathlib import Path

import click

import qq_lib
from qq_lib.batch import QQBatchInterface, QQBatchMeta
from qq_lib.common import get_info_file
from qq_lib.constants import (
    GUARD,
    INFO_FILE,
)
from qq_lib.error import QQError
from qq_lib.info import QQInfo, QQInformer
from qq_lib.logger import get_logger
from qq_lib.pbs import QQPBS
from qq_lib.resources import QQResources
from qq_lib.constants import QQ_INFO_SUFFIX, QQ_SUFFIXES, STDERR_SUFFIX, STDOUT_SUFFIX
from qq_lib.states import NaiveState, RealState

logger = get_logger(__name__)


@click.command(short_help = "Submit a qq job to the batch system.")
@click.argument("queue", type=str)
@click.argument("script", type=str)
@click.option("--ncpus", type=int, default=None)
@click.option("--vnode", type=str, default=None)
@click.option("--walltime", type=str, default=None)
@click.option("--work-dir", type=str, default="scratch_local")
@click.option("--work-size", type=str, default=None)
@click.option("--batch-system", type=str, default = "PBS")
def submit(queue, script, **kwargs):
    """
    Submit a qq job to a batch system from the command line.

    Note that the submitted script must be located in the same 
    directory from which 'qq submit' is invoked.

    Exits:
        0 on successful submission,
        91 if a QQError occurs,
        99 on unexpected exceptions.
    """
    try:
        BatchSystem = QQBatchMeta.fromStr(kwargs["batch_system"])
        del kwargs["batch_system"]
        resources = QQResources(**kwargs)
        submitter = QQSubmitter(BatchSystem, queue, Path(script), resources)
        submitter.guard()
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

        self.batch_system = batch_system
        self.queue = queue
        self.script = script
        self.script_name = script.name # strip any potential absolute path
        self.info_file = self.script.with_suffix(QQ_INFO_SUFFIX).resolve()
        self.resources = resources

        # script must exist
        if not self.script.is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")
        
        # script must exist in the current directory
        if not Path(self.script_name).is_file():
            raise QQError(f"Script '{script}' is not in the submission directory.")

        if not self._hasValidShebang(self.script):
            raise QQError(
                f"Script '{self.script}' has an invalid shebang. The first line of the script should be '#!/usr/bin/env -S qq run'."
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
        result = self.batch_system.jobSubmit(self.resources, self.queue, self.script)

        if result.exit_code == 0:
            # submission succesful
            job_id = result.success_message
            logger.info(f"Job '{job_id}' submitted successfully.")
            informer = QQInformer(
                QQInfo(
                    batch_system = self.batch_system,
                    qq_version = qq_lib.__version__,
                    username = getpass.getuser(),
                    job_id = job_id,
                    job_name = self.script_name,
                    script_name = self.script_name,
                    job_type = "standard",
                    input_machine = socket.gethostname(),
                    job_dir = Path.cwd(),
                    job_state = NaiveState.QUEUED,
                    submission_time = datetime.now(),
                    stdout_file = str(Path(self.script_name).with_suffix(STDOUT_SUFFIX)),
                    stderr_file = str(Path(self.script_name).with_suffix(STDERR_SUFFIX)),
                    resources = self.resources,
                )
            )

            informer.toFile(self.info_file)
            return job_id
        else:
            # submission failed
            raise QQError(f"Failed to submit script '{self.script}': {result.error_message}.")

    def guard(self):
        """
        Prevent multiple submissions from the same directory.

        Raises:
            QQError: If qq runtime files are detected in the current directory.
        """
        if not self._qqFilesPresent():
            return  # no qq files present, all good

        # weaker warning
        error_msg_soft = (
            "Detected qq runtime files, likely from an invalid or failed run. "
            "Submission not allowed until these files are cleared. "
            "To clear the files, run 'qq clear'."
        )

        # attempt to locate a single qq info file
        try:
            info_file = get_info_file(Path.cwd())
            informer = QQInformer.fromFile(info_file)
        except QQError as e:
            # no, multiple, or an invalid qq info file
            logger.debug(e)
            raise QQError(error_msg_soft)

        unproblematic_states = {
            RealState.KILLED,
            RealState.FAILED,
            RealState.IN_AN_INCONSISTENT_STATE,
        }

        if informer.getRealState() in unproblematic_states:
            # job is killed, failed, or in an inconsistent state
            raise QQError(error_msg_soft)

        # job is active or successfully finished -- stronger warning
        raise QQError(
            "Detected qq runtime files from an active or successfully finished job. "
            "SUBMISSION NOT ALLOWED! "
            "If you know what you are doing, run 'qq clear --force'."
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
        os.environ[INFO_FILE] = str(self.info_file)

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
        