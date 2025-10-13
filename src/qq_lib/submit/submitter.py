# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import getpass
import os
import socket
from datetime import datetime
from pathlib import Path

import qq_lib
from qq_lib.batch.interface import QQBatchInterface
from qq_lib.core.common import get_info_file
from qq_lib.core.constants import (
    ARCHIVE_FORMAT,
    BATCH_SYSTEM,
    GUARD,
    INFO_FILE,
    INPUT_DIR,
    INPUT_MACHINE,
    LOOP_CURRENT,
    LOOP_END,
    LOOP_JOB_PATTERN,
    LOOP_START,
    QQ_INFO_SUFFIX,
    QQ_SUFFIXES,
    STDERR_SUFFIX,
    STDOUT_SUFFIX,
)
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
        script: Path,
        job_type: QQJobType,
        resources: QQResources,
        command_line: list[str],
        loop_info: QQLoopInfo | None = None,
        exclude: list[Path] | None = None,
        depend: list[Depend] | None = None,
        interactive: bool = True,
    ):
        """
        Initialize a QQSubmitter instance.

        Args:
            batch_system (type[QQBatchInterface]): The batch system class implementing
                the QQBatchInterface used for job submission.
            queue (str): The name of the batch system queue to which the job will be submitted.
            script (Path): Path to the job script to submit. Must exist, be located in
                the current working directory, and have a valid shebang.
            job_type (QQJobType): Type of the job to submit (e.g. standard, loop).
            resources (QQResources): Job resource requirements (e.g., CPUs, memory, walltime).
            command_line (list[str]): List of all arguments and options provided on the command line.
            loop_info (QQLoopInfo | None): Optional information for loop jobs. Pass None if not applicable.
            exclude (list[Path] | None): Optional list of files which should not be copied to the working directory.
            depend (list[Depend] | None): Optional list of job dependencies.
            interactive (bool): Is the submitter used in an interactive mode? Defaults to True.

        Raises:
            QQError: If the script does not exist, is not in the current directory,
                or has an invalid shebang line.
        """

        self._batch_system = batch_system
        self._job_type = job_type
        self._queue = queue
        self._loop_info = loop_info
        self._script = script
        self._script_name = script.name  # strip any potential absolute path
        self._job_name = self._constructJobName()
        self._info_file = Path(self._job_name).with_suffix(QQ_INFO_SUFFIX).resolve()
        self._resources = resources
        self._exclude = exclude or []
        self._command_line = command_line
        self._depend = depend or []
        self._interactive = interactive

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
            str: The job ID of the submitted job.

        Raises:
            QQError: If job submission fails.
        """
        # setting the basic environment variables for communicating with `qq run`
        self._setEnvVars()

        # submit the job
        job_id = self._batch_system.jobSubmit(
            self._resources,
            self._queue,
            self._script,
            self._job_name,
            self._depend,
        )

        # create info file
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
                input_dir=Path.cwd(),
                job_state=NaiveState.QUEUED,
                submission_time=datetime.now(),
                stdout_file=str(Path(self._job_name).with_suffix(STDOUT_SUFFIX)),
                stderr_file=str(Path(self._job_name).with_suffix(STDERR_SUFFIX)),
                resources=self._resources,
                loop_info=self._loop_info,
                excluded_files=self._exclude,
                command_line=self._command_line,
                depend=self._depend,
            )
        )

        informer.toFile(self._info_file)
        return job_id

    def guardOrClear(self) -> None:
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

        # if this is a loop job and the qq runtime files are from a previous cycle, we do not clear them
        # we want to archive these files in the next cycle of the loop job
        if self._shouldSkipClear():
            return

        # if we are in a non-interactive environment, any detection of qq runtime files is a hard error
        if not self._interactive:
            raise QQError(
                "Detected qq runtime files in the submission directory. Submission aborted."
            )

        # attempt to clear the files or raise an error
        """clearer = QQClearer(Path.cwd())
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
            )"""

    def _shouldSkipClear(self) -> bool:
        """
        Determine whether clearing of files should be skipped for a loop job.

        Returns:
            bool:
                - True if clearing should be skipped because the previous cycle has
                completed successfully.
                - False otherwise, including when the job is not a loop job, no info file
                is found, multiple info files are detected, or the file cannot be read.
        """
        # this is not a loop job
        if not self._info_file:
            logger.debug("Not a loop job.")
            return False

        try:
            # to skip file clearing, there can only be a single info file
            # corresponding to the previous cycle of the loop job
            # that is already finished
            info_file = get_info_file(Path.cwd())
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
            logger.debug(f"Could not read a valid info file: {e}.")
            return False

    def _qqFilesPresent(self) -> bool:
        """
        Check for presence of qq runtime files in the current directory.

        Returns:
            bool: True if files with QQ_SUFFIXES are present, False otherwise.
        """
        current_dir = Path()
        for file in current_dir.iterdir():
            if file.is_file() and file.suffix in QQ_SUFFIXES:
                return True
        return False

    def _setEnvVars(self) -> None:
        """
        Set environment variables required for qq runtime.
        """
        # this indicates that the job is running in a qq environment
        os.environ[GUARD] = "true"

        # this contains a path to the qq info file
        os.environ[INFO_FILE] = str(self._info_file)

        # this contains the name of the input host
        os.environ[INPUT_MACHINE] = socket.gethostname()

        # this contains the name of the used batch system
        os.environ[BATCH_SYSTEM] = str(self._batch_system)

        # this contains the path to the input directory
        os.environ[INPUT_DIR] = str(Path.cwd())

        # loop job-specific environment variables
        if self._loop_info:
            os.environ[LOOP_CURRENT] = str(self._loop_info.current)
            os.environ[LOOP_START] = str(self._loop_info.start)
            os.environ[LOOP_END] = str(self._loop_info.end)
            os.environ[ARCHIVE_FORMAT] = self._loop_info.archive_format

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
            return first_line.startswith("#!") and first_line.strip().endswith("qq run")

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
        return f"{self._script_name}{LOOP_JOB_PATTERN % self._loop_info.current}"
