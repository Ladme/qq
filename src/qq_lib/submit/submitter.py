# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import getpass
import os
import socket
from datetime import datetime
from pathlib import Path

import qq_lib
from qq_lib.batch.interface import QQBatchInterface
from qq_lib.core.common import get_info_file, hhmmss_to_duration
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import QQInformer
from qq_lib.properties.depend import Depend
from qq_lib.properties.info import QQInfo
from qq_lib.properties.job_type import QQJobType
from qq_lib.properties.loop import QQLoopInfo
from qq_lib.properties.resources import QQResources
from qq_lib.properties.states import NaiveState

logger = get_logger(__name__)


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
        account: str | None,
        script: Path,
        job_type: QQJobType,
        resources: QQResources,
        command_line: list[str],
        loop_info: QQLoopInfo | None = None,
        exclude: list[Path] | None = None,
        depend: list[Depend] | None = None,
    ):
        """
        Initialize a QQSubmitter instance.

        Args:
            batch_system (type[QQBatchInterface]): The batch system class implementing
                the QQBatchInterface used for job submission.
            queue (str): The name of the batch system queue to which the job will be submitted.
            account (str | None): The name of the account to use for the job.
            script (Path): Path to the job script to submit.
            job_type (QQJobType): Type of the job to submit (e.g. standard, loop).
            resources (QQResources): Job resource requirements (e.g., CPUs, memory, walltime).
            command_line (list[str]): List of all arguments and options provided on the command line.
            loop_info (QQLoopInfo | None): Optional information for loop jobs. Pass None if not applicable.
            exclude (list[Path] | None): Optional list of files which should not be copied to the working directory.
            depend (list[Depend] | None): Optional list of job dependencies.

        Raises:
            QQError: If the script does not exist or has an invalid shebang line.
        """

        self._batch_system = batch_system
        self._job_type = job_type
        self._queue = queue
        self._account = account
        self._loop_info = loop_info
        self._script = script
        self._input_dir = script.resolve().parent
        self._script_name = script.name
        self._job_name = self._constructJobName()
        self._info_file = (
            (self._input_dir / self._job_name)
            .with_suffix(CFG.suffixes.qq_info)
            .resolve()
        )
        self._resources = resources
        self._exclude = exclude or []
        self._command_line = command_line
        self._depend = depend or []

        # script must exist
        if not self._script.is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

        # script must have a valid qq shebang
        if not self._hasValidShebang(self._script):
            raise QQError(
                f"Script '{self._script}' has an invalid shebang. The first line of the script should be '#!/usr/bin/env -S {CFG.binary_name} run'."
            )

    def submit(self) -> str:
        """
        Submit the script to the batch system.

        Sets required environment variables, calls the batch system's
        job submission mechanism, and creates a QQInfo file with job metadata.

        Returns:
            str: The job ID of the submitted job.

        Raises:
            QQError: If job submission fails.
        """
        # submit the job
        job_id = self._batch_system.jobSubmit(
            self._resources,
            self._queue,
            self._script,
            self._job_name,
            self._depend,
            self._createEnvVarsDict(),
            self._account,
        )

        # create job qq info file
        informer = QQInformer(
            QQInfo(
                batch_system=self._batch_system,
                qq_version=qq_lib.__version__,
                username=getpass.getuser(),
                job_id=job_id,
                job_name=self._job_name,
                script_name=self._script_name,
                queue=self._queue,
                job_type=self._job_type,
                input_machine=socket.gethostname(),
                input_dir=self._input_dir,
                job_state=NaiveState.QUEUED,
                submission_time=datetime.now(),
                stdout_file=str(Path(self._job_name).with_suffix(CFG.suffixes.stdout)),
                stderr_file=str(Path(self._job_name).with_suffix(CFG.suffixes.stderr)),
                resources=self._resources,
                loop_info=self._loop_info,
                excluded_files=self._exclude,
                command_line=self._command_line,
                depend=self._depend,
                account=self._account,
            )
        )
        informer.toFile(self._info_file)
        return job_id

    def continuesLoop(self) -> bool:
        """
        Determine whether the submitted job is a continuation of a loop job.

        Checks if an info file exists in the input directory that corresponds
        to the previous cycle of the same loop job. A job is considered a valid
        continuation if:
          - An info file is found.
          - Both the info file and the current job are loop jobs.
          - The previous job finished successfully.
          - The previous loop cycle number is exactly one less than the current one.

        Returns:
            bool: True if the job is a valid continuation of a previous loop job,
                  False otherwise.
        """
        try:
            # only one qq info file can be present
            info_file = get_info_file(self._input_dir)
            informer = QQInformer.fromFile(info_file)

            if (
                informer.info.loop_info
                and self._loop_info
                and informer.info.job_state == NaiveState.FINISHED
                and informer.info.loop_info.current == self._loop_info.current - 1
            ):
                logger.debug("Valid loop job with a correct cycle.")
                return True
            logger.debug(
                "Detected info file is either not a loop job or does not correspond to the previous cycle."
            )
            return False
        except QQError as e:
            logger.debug(f"Could not read an info file: {e}.")
            return False

    def getInputDir(self) -> Path:
        """
        Get path to the job's input directory.

        Returns:
            Path: Path to the job's input directory.
        """
        return self._input_dir

    def _createEnvVarsDict(self) -> dict[str, str]:
        """
        Create a dictionary of environment variables provided to qq runtime.

        Returns
            dict[str, str]: Dictionary of environment variables and their values.
        """
        env_vars = {}

        # propagate qq debug environment
        if os.environ.get(CFG.env_vars.debug_mode):
            env_vars[CFG.env_vars.debug_mode] = "true"

        # indicates that the job is running in a qq environment
        env_vars[CFG.env_vars.guard] = "true"

        # contains a path to the qq info file
        env_vars[CFG.env_vars.info_file] = str(self._info_file)

        # contains the name of the input host
        env_vars[CFG.env_vars.input_machine] = socket.gethostname()

        # contains the name of the used batch system
        env_vars[CFG.env_vars.batch_system] = str(self._batch_system)

        # contains the path to the input directory
        env_vars[CFG.env_vars.input_dir] = str(self._input_dir)

        # environment variables for resources
        env_vars[CFG.env_vars.ncpus] = str(self._resources.ncpus or 1)
        env_vars[CFG.env_vars.ngpus] = str(self._resources.ngpus or 0)
        env_vars[CFG.env_vars.nnodes] = str(self._resources.nnodes or 1)
        env_vars[CFG.env_vars.walltime] = str(
            hhmmss_to_duration(self._resources.walltime or "00:00:00").total_seconds()
            / 3600
        )

        # loop job-specific environment variables
        if self._loop_info:
            env_vars[CFG.env_vars.loop_current] = str(self._loop_info.current)
            env_vars[CFG.env_vars.loop_start] = str(self._loop_info.start)
            env_vars[CFG.env_vars.loop_end] = str(self._loop_info.end)
            env_vars[CFG.env_vars.archive_format] = self._loop_info.archive_format
            env_vars[CFG.env_vars.no_resubmit] = str(CFG.exit_codes.qq_run_no_resubmit)

        return env_vars

    def _hasValidShebang(self, script: Path) -> bool:
        """
        Verify that the script has a valid shebang for qq run.

        Args:
            script (Path): Path to the script file.

        Returns:
            bool: True if the first line starts with '#!' and ends with 'qq run'.
        """
        with Path.open(script) as file:
            first_line = file.readline()
            return first_line.startswith("#!") and first_line.strip().endswith(
                f"{CFG.binary_name} run"
            )

    def _constructJobName(self) -> str:
        """
        Construct the job name for submission.

        Returns:
            str: The constructed job name.
        """
        # for standard jobs, use script name
        if not self._loop_info:
            return self._script_name

        # for loop jobs, use script_name with cycle number
        return f"{self._script_name}{CFG.loop_jobs.pattern % self._loop_info.current}"
