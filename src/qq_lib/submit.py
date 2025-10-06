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
from click_option_group import optgroup

import qq_lib
from qq_lib.batch import QQBatchInterface
from qq_lib.clear import QQClearer
from qq_lib.click_format import GNUHelpColorsCommand
from qq_lib.common import get_info_file, yes_or_no_prompt
from qq_lib.constants import (
    ARCHIVE_FORMAT,
    BATCH_SYSTEM,
    GUARD,
    INFO_FILE,
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
from qq_lib.error import QQError
from qq_lib.info import QQInfo, QQInformer
from qq_lib.job_type import QQJobType
from qq_lib.logger import get_logger
from qq_lib.loop import QQLoopInfo
from qq_lib.resources import QQResources
from qq_lib.states import NaiveState

logger = get_logger(__name__)


# Note that all options must be part of an optgroup otherwise QQParser breaks.
@click.command(
    short_help="Submit a qq job to the batch system.",
    help=f"""
Submit a qq job to a batch system from the command line.

{click.style("SCRIPT", fg="green")}   Path to the script to submit.

The submitted script must be located in the directory from which
'qq submit' is invoked.
""",
    cls=GNUHelpColorsCommand,
    help_options_color="blue",
)
@click.argument("script", type=str, metavar=click.style("SCRIPT", fg="green"))
@optgroup.group(f"{click.style('General settings', fg='yellow')}")
@optgroup.option(
    "--queue",
    "-q",
    type=str,
    default=None,
    help=f"Name of the queue to submit the job to. {click.style('Required.', bold=True)}",
)
@optgroup.option(
    "--job-type",
    type=str,
    default=None,
    help="Type of the qq job. Defaults to 'standard'.",
)
@optgroup.option(
    "--exclude",
    type=str,
    default=None,
    help=(
        f"A colon-, comma-, or space-separated list of files and directories that should {click.style('not', bold=True)} be copied to the working directory.\n"
        "     By default, all files and directories except for the qq info file and the archive directory are copied to the working directory.\n"
    ),
)
@optgroup.option(
    "--batch-system",
    type=str,
    default=None,
    help=f"Batch system to submit the job into. If not specified, will load the batch system from the environment variable '{BATCH_SYSTEM}' or guess it.",
)
@optgroup.option(
    "--non-interactive",
    is_flag=True,
    help="Use when using qq submit in a non-interactive environment. Any interactive prompt will be automatically skipped and evaluated as false.",
)
@optgroup.group(f"{click.style('Requested resources', fg='yellow')}")
@optgroup.option(
    "--nnodes", type=int, default=None, help="Number of computing nodes to use."
)
@optgroup.option(
    "--ncpus",
    type=int,
    default=None,
    help="Number of CPU cores to use.",
)
@optgroup.option(
    "--mem-per-cpu",
    type=str,
    default=None,
    help="Amount of memory to use per a single CPU core. Specify as 'Nmb' or 'Ngb' (e.g., 500mb or 2gb).",
)
@optgroup.option(
    "--mem",
    type=str,
    default=None,
    help="Absolute amount of memory to use. Specify as 'Nmb' or 'Ngb' (e.g., 500mb or 10gb). Overrides '--mem-per-cpu'.",
)
@optgroup.option("--ngpus", type=int, default=None, help="Number of GPUs to use.")
@optgroup.option(
    "--walltime",
    type=str,
    default=None,
    help="Maximum allowed runtime for the job.",
)
@optgroup.option(
    "--work-dir",
    "--workdir",
    type=str,
    default=None,
    help="Type of working directory to use.",
)
@optgroup.option(
    "--work-size-per-cpu",
    "--worksize-per-cpu",
    type=str,
    default=None,
    help="Size of the storage requested for running the job per a single CPU core. Specify as 'Ngb' (e.g., 1gb).",
)
@optgroup.option(
    "--work-size",
    "--worksize",
    type=str,
    default=None,
    help="Absolute size of the storage requested for running the job. Specify as 'Ngb' (e.g., 10gb). Overrides '--work-size-per-cpu'.",
)
@optgroup.option(
    "--props",
    type=str,
    default=None,
    help="A colon-, comma-, or space-separated list of properties that a node must include (e.g., cl_two) or exclude (e.g., ^cl_two) in order to run the job.",
)
@optgroup.group(
    f"{click.style('Loop options', fg='yellow')}",
    help="Only used when job-type is 'loop'.",
)
@optgroup.option(
    "--loop-start",
    type=int,
    default=None,
    help="The first cycle of the loop job. Defaults to 1.",
)
@optgroup.option(
    "--loop-end", type=int, default=None, help="The last cycle of the loop job."
)
@optgroup.option(
    "--archive",
    type=str,
    default=None,
    help="Name of the directory for archiving files from the loop job. Defaults to 'storage'.",
)
@optgroup.option(
    "--archive-format",
    type=str,
    default=None,
    help="Format of the archived filenames. Defaults to 'job%04d'.",
)
def submit(script: str, **kwargs):
    """
    Submit a qq job to a batch system from the command line.

    Note that the submitted script must be located in the same directory from which 'qq submit' is invoked.
    """
    from qq_lib.submit_factory import QQSubmitterFactory

    try:
        if not (script_path := Path(script)).is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

        # parse options from the command line and from the script itself
        factory = QQSubmitterFactory(
            script_path.resolve(), submit.params, sys.argv[2:], **kwargs
        )
        submitter = factory.makeSubmitter()

        # catching multiple submissions
        submitter.guardOrClear()

        job_id = submitter.submit()
        logger.info(f"Job '{job_id}' submitted successfully.")
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
        job_type: QQJobType,
        resources: QQResources,
        loop_info: QQLoopInfo | None,
        exclude: list[Path],
        command_line: list[str],
        interactive: bool,
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
            loop_info (QQLoopInfo | None): Optional information for loop jobs. Pass None if not applicable.
            exclude (list[Path]): Files which should not be copied to the working directory.
            command_line (list[str]): List of all arguments and options provided on the command line.
            interactive (bool): Is the submitter used in an interactive mode?

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
        self._exclude = exclude
        self._command_line = command_line
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
            self._resources, self._queue, self._script, self._job_name
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
                job_dir=Path.cwd(),
                job_state=NaiveState.QUEUED,
                submission_time=datetime.now(),
                stdout_file=str(Path(self._job_name).with_suffix(STDOUT_SUFFIX)),
                stderr_file=str(Path(self._job_name).with_suffix(STDERR_SUFFIX)),
                resources=self._resources,
                loop_info=self._loop_info,
                excluded_files=self._exclude,
                command_line=self._command_line,
            )
        )

        informer.toFile(self._info_file)
        return job_id

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

    def _setEnvVars(self):
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
