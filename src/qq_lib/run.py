# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
This module defines the `QQRunner` class and related helpers that manage the
execution of qq jobs within a batch system. It is invoked internally through
the `qq run` command, which is hidden from the user-facing CLI.

Lifecycle of a qq job:
    1. Working directory preparation
       - Shared storage jobs: The working directory is set to the job
         submission directory itself.
       - Scratch-using jobs: A dedicated scratch directory (created by the
         batch system) is used as a working directory. Job files are copied
         to the working directory.

    2. Execution
       The qq info file is updated to record the "running" state.
       The job script is executed.

    3. Completion handling
       - On success:
         - The qq info file is updated to "finished".
         - If running on scratch, job files are copied back to the submission
           (job) directory and then removed from scratch.
       - On failure:
         - The qq info file is updated to "failed".
         - If on scratch, files are left in place for debugging.

    X. Cleanup (on interruption)
       If the process receives a SIGTERM, the runner updates the qq info file
       to "killed", attempts to gracefully terminate the subprocess, and forces
       termination with SIGKILL if necessary.

Summary:
    - Shared-storage jobs execute directly in the job directory, with no
      file copying.
    - Scratch-using jobs copy job files to scratch, execute there, and then
      either copy results back (on success) or leave scratch data intact (on
      failure).
"""

import os
import shutil
import signal
import socket
import subprocess
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import NoReturn

import click

from qq_lib.common import convert_to_batch_system
from qq_lib.env_vars import (
    BATCH_SYSTEM,
    INFO_FILE,
    JOBDIR,
    STDERR_FILE,
    STDOUT_FILE,
    USE_SCRATCH,
    WORKDIR,
)
from qq_lib.error import QQError
from qq_lib.guard import guard
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger

# time in seconds between sending a SIGTERM signal
# to the running process and sending a SIGKILL signal
SIGTERM_TO_SIGKILL = 5

logger = get_logger(__name__)


@click.command(
    hidden=True,
    help="Execute a script inside qq batch environment. Do not call directly.",
)
@click.argument("script_path", type=str)
def run(script_path: str):
    """
    Entrypoint for executing a script inside the qq batch environment.

    This function:
      - Ensures the script is running in a batch job context
      - Prepares the job working directory (scratch or shared)
      - Executes the script and handles exit codes
      - Logs errors or unexpected failures into the qq info file

    Args:
        script_path (str): Path to the script.
            Note: This argument is ignored because the batch system provides
            a temporary copy. The original script in the working directory
            is used instead.

    Raises:
        SystemExit: Exits with the script's exit code, or with specific
            error codes:
              - 91: Guard check failure or an error logged into an info file
              - 92: Fatal error not logged into an info file
              - 99: Fatal unexpected error (indicates a bug)
              - 143: Execution terminated by SIGTERM.
    """

    # the script path provided points to a script copied to a temporary
    # location by the batch system => we ignore it and later use the
    # 'original' script in the working directory
    _ = script_path

    # make sure that qq run is being run as a batch job
    try:
        guard()
    except Exception as e:
        logger.error(e)
        sys.exit(91)

    # initialize the runner performing only the most necessary operations
    try:
        runner = QQRunner()
    except QQError as e:
        # the most basic setup of the run failed
        # can't even log the failure state to the info file
        _log_fatal_qq_error(e, 92)  # exits here
    except Exception as e:
        _log_fatal_unexpected_error(e, 99)  # exits here

    # prepare the working directory, execute the script and perform clean-up
    try:
        runner.setUp()
        runner.setUpWorkDir()
        exit_code = runner.executeScript()
        sys.exit(exit_code)
    except QQError as e:
        # if the execution fails, log this error into both stderr and the info file
        logger.error(e)
        runner.logFailureIntoInfoFile(91)  # exits here
    except Exception as e:
        # even unknown exceptions should be logged into both stderr and the info file
        # this indicates a bug in the program
        logger.critical(e, exc_info=True, stack_info=True)
        runner.logFailureIntoInfoFile(99)  # exits here


class QQRunner:
    """
    Manages the setup, execution, and cleanup of scripts within the qq batch environment.

    The QQRunner class is responsible for:
      - Preparing a working directory (shared or scratch space)
      - Executing a provided job script
      - Updating the job info file with run state, success, or failure
      - Cleaning up resources when execution is finished
    """

    def __init__(self):
        """
        Initialize a new QQRunner instance.

        This performs only minimal setup:
          - Installs a signal handler for SIGTERM
          - Locates and loads the qq info file

        If any of these steps fail, a fatal error is raised without updating the
        qq info file (since it may not yet be available).

        Raises:
            QQError: If the info file cannot be found or loaded.
        """
        self.process: subprocess.Popen[str] | None = None

        # install a signal handler
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        # get path to the qq info file
        self.info_file = os.environ.get(INFO_FILE)
        if not self.info_file:
            raise QQError(f"'{INFO_FILE}' environment variable is not set.")
        logger.debug(f"Info file: '{self.info_file}'.")

        # load the info file
        self.info = QQInformer.loadFromFile(self.info_file)

    def setUp(self):
        """
        Perform the full setup of the QQRunner.

        Raises:
            QQError: If required environment variables are missing or invalid.
        """
        # get job directory
        job_dir = os.environ.get(JOBDIR)
        if not job_dir:
            raise QQError(f"'{JOBDIR}' environment variable is not set.")
        logger.debug(f"Job directory: '{job_dir}'.")

        self.job_dir = Path(job_dir)

        # get the batch system
        batch_system_name = os.environ.get(BATCH_SYSTEM)
        if not batch_system_name:
            raise QQError(f"Required '{BATCH_SYSTEM}' environment variable is not set.")
        logger.debug(f"Used batch system: '{batch_system_name}'.")

        try:
            self.batch_system = convert_to_batch_system(batch_system_name)
        except KeyError as e:
            raise QQError(
                f"Unknown batch system name '{batch_system_name}': {e}"
            ) from e

        # get the username and job id
        self.username = os.environ.get(self.batch_system.usernameEnvVar())
        self.jobid = os.environ.get(self.batch_system.jobIdEnvVar())

        if not self.username or not self.jobid:
            raise QQError(
                f"Required '{self.batch_system.envName()}' environment variables are not set."
            )

        logger.debug(f"Username: {self.username}.")
        logger.debug(f"Job ID: {self.jobid}.")

        # should the scratch directory be used?
        self.use_scratch = os.environ.get(USE_SCRATCH) is not None

        logger.debug(f"Use scratch: {self.use_scratch}.")

    def setUpWorkDir(self):
        """
        Prepare the working directory for job execution.

        Depending on configuration, this sets up:
          - A scratch directory (copying job files there), or
          - The shared job directory directly.

        Raises:
            QQError: If working directory setup fails.
        """
        if self.use_scratch:
            self._setUpScratchDir()
        else:
            self._setUpSharedDir()

    def executeScript(self) -> int:
        """
        Execute the job script in the working directory.

        Handles scratch directory copying and cleaning after the execution.

        Returns:
            int: The exit code from the executed script.

        Raises:
            QQError: If execution fails or required environment variables are missing.
        """
        # update the qqinfo file
        self._updateInfoRun()

        # get the actual name of the script to execute
        script = Path(self.info.getJobName()).resolve()

        # get paths to output files
        stdout_log = os.environ.get(STDOUT_FILE)
        stderr_log = os.environ.get(STDERR_FILE)
        if not stdout_log or not stderr_log:
            raise QQError(
                f"'{STDOUT_FILE}' or '{STDERR_FILE}' environment variable has not been set up."
            )

        with script.open() as file:
            lines = file.readlines()[1:]

        try:
            with Path.open(stdout_log, "w") as out, Path.open(stderr_log, "w") as err:
                self.process = subprocess.Popen(
                    ["bash"],
                    stdin=subprocess.PIPE,
                    stdout=out,
                    stderr=err,
                    text=True,
                )
                self.process.communicate(input="".join(lines))

        except Exception as e:
            raise QQError(f"Failed to execute script '{script}': {e}") from e

        if self.use_scratch:
            # copy files back to the submission (job) directory
            self._copyFilesFromWorkDir()

        # update the qqinfo file
        if self.process.returncode == 0:
            self._updateInfoFinished()
            if self.use_scratch:
                # remove the files from scratch
                # files are retained on scratch if the run fails for any reason
                self._removeFilesFromWorkDir()
        else:
            self._updateInfoFailed(self.process.returncode)

        return self.process.returncode

    def logFailureIntoInfoFile(self, exit_code: int) -> NoReturn:
        """
        Record a failure state into the qq info file and exit the program.

        Args:
            exit_code (int): The exit code to record and use when terminating the program.

        Raises:
            SystemExit: Always exits with the given exit code.
        """
        try:
            self._updateInfoFailed(exit_code)
            sys.exit(exit_code)
        except QQError as e:
            if exit_code == 99:
                _log_fatal_unexpected_error(e, exit_code)
            _log_fatal_qq_error(e, exit_code)
        except Exception as e:
            _log_fatal_unexpected_error(e, exit_code)

    def _setUpSharedDir(self):
        """
        Configure the job directory as the working directory.
        """
        # set qq working directory to job directory
        self.work_dir = self.job_dir

        # export working directory path as an environment variable
        os.environ[WORKDIR] = str(self.work_dir)

        # move to the working directory
        os.chdir(self.work_dir)

    def _setUpScratchDir(self):
        """
        Configure a scratch directory as the working directory.

        Copies all files from the job directory to the working directory
        (excluding the qq info file).

        Raises:
            QQError: If scratch directory cannot be determined.
        """
        # set qq working directory to scratch directory
        work_dir = os.environ.get(self.batch_system.scratchDirEnvVar())
        if not work_dir:
            raise QQError(
                f"Could not get the scratch directory from '{self.batch_system.scratchDirEnvVar()}' environment variable."
            )
        self.work_dir = Path(work_dir)

        # export working directory path as an environment variable
        os.environ[WORKDIR] = str(self.work_dir)

        # move to the working directory
        os.chdir(self.work_dir)

        # copy files to the working directory excluding the qq info file
        self._copyFilesToDst(
            self._getFilesToCopy(self.job_dir, [self.info_file]), self.work_dir
        )

    def _copyFilesToDst(self, src: Sequence[Path], directory: Path):
        """
        Copy multiple files or directories into a destination directory.

        Args:
            src (Sequence[Path]): Paths of files/directories to copy.
            directory (Path): Destination directory.

        Raises:
            QQError: If any file cannot be copied.
        """
        for item in src:
            dest = directory / item.name
            logger.debug(f"Copying '{item}' to '{dest}'.")

            try:
                if item.is_file():
                    shutil.copy2(item, dest)
                elif item.is_dir():
                    shutil.copytree(item, dest, dirs_exist_ok=True)
                else:
                    logger.warning(
                        f"Not copying '{item}' to working directory: not a file or directory."
                    )
            except Exception as e:
                raise QQError(
                    f"Could not copy '{item}' into directory '{directory}': {e}."
                ) from e

    def _copyFilesFromWorkDir(self):
        """
        Copy all files from the working directory back into the job directory.
        """
        logger.debug(f"Copying files from {self.work_dir} to {self.job_dir}.")
        files_to_copy = self._getFilesToCopy(self.work_dir, [])
        logger.debug(f"Copying the following paths: {files_to_copy}.")
        self._copyFilesToDst(files_to_copy, self.job_dir)

    def _removeFilesFromWorkDir(self):
        """
        Delete all files and directories from the working directory.

        Used only after successful execution in scratch space.
        """
        for item in self.work_dir.iterdir():
            if item.is_file() or item.is_symlink():
                logger.debug(f"Removing file {item}.")
                item.unlink()
            elif item.is_dir():
                logger.debug(f"Removing directory {item}.")
                shutil.rmtree(item)

    def _getFilesToCopy(
        self, directory: Path, filter_out: list[str] | None = None
    ) -> list[Path]:
        """
        List files and directories in a source directory, excluding certain paths.

        Args:
            directory (Path): Source directory to scan.
            filter_out (list[str] | None): Paths to exclude.

        Returns:
            list[Path]: Paths of files/directories to be copied.
        """
        root = Path(directory).resolve()
        # normalize filter_out to absolute paths
        filter_paths = {Path(p).resolve() for p in filter_out} if filter_out else set()
        logger.debug(f"Paths to filter out: {filter_paths}.")

        return [p for p in root.iterdir() if p.resolve() not in filter_paths]

    def _updateInfoRun(self):
        """
        Update the qq info file to mark the job as running.

        Raises:
            QQError: If the info file cannot be updated.
        """
        logger.debug(f"Updating '{self.info_file}' at job start.")
        try:
            self.info.setRunning(datetime.now(), socket.gethostname(), self.work_dir)
            self.info.exportToFile(self.info_file)
        except Exception as e:
            raise QQError(
                f"Could not update qqinfo file '{self.info_file}' at JOB START: {e}."
            ) from e

    def _updateInfoFinished(self):
        """
        Update the qq info file to mark the job as successfully finished.

        Logs errors as warnings if updating fails.
        """
        logger.debug(f"Updating '{self.info_file}' at job completion.")
        try:
            self.info.setFinished(datetime.now())
            self.info.exportToFile(self.info_file)
        except Exception as e:
            logger.warning(
                f"Could not update qqinfo file '{self.info_file}' at JOB COMPLETION: {e}."
            )

    def _updateInfoFailed(self, return_code: int):
        """
        Update the qq info file to mark the job as failed.

        Args:
            return_code (int): Exit code from the failed job.

        Logs errors as warnings if updating fails.
        """
        logger.debug(f"Updating '{self.info_file}' at job failure.")
        try:
            self.info.setFailed(datetime.now(), return_code)
            self.info.exportToFile(self.info_file)
        except Exception as e:
            logger.warning(
                f"Could not update qqinfo file '{self.info_file}' at JOB FAILURE: {e}."
            )

    def _updateInfoKilled(self):
        """
        Update the qq info file to mark the job as killed.

        Used during SIGTERM cleanup.

        Logs errors as warnings if updating fails.
        """
        logger.debug(f"Updating '{self.info_file}' at job kill.")
        try:
            self.info.setKilled(datetime.now())
            self.info.exportToFile(self.info_file)
        except Exception as e:
            logger.warning(
                f"Could not update qqinfo file '{self.info_file}' at JOB KILL: {e}."
            )

    def _cleanup(self) -> None:
        """
        Clean up after execution is interrupted or killed.

        - Marks job as killed in the info file
        - Terminates the subprocess
        """
        # update the qq info file
        self._updateInfoKilled()

        # send SIGTERM to the running process, if there is any
        if self.process and self.process.poll() is None:
            logger.info("Cleaning up: terminating subprocess.")
            self.process.terminate()
            try:
                self.process.wait(timeout=SIGTERM_TO_SIGKILL)
            # kill the running process if this takes too long
            except subprocess.TimeoutExpired:
                logger.info("Subprocess did not exit, killing.")
                self.process.kill()

    def _handle_sigterm(self, _signum: int, _frame: FrameType | None) -> None:
        """
        Signal handler for SIGTERM.

        Performs cleanup, logs termination, and exits with code 15.
        """
        logger.info("Received SIGTERM, initiating shutdown.")
        self._cleanup()
        logger.error("Execution was terminated by SIGTERM.")
        sys.exit(143)


def _log_fatal_qq_error(exception: QQError, exit_code: int) -> NoReturn:
    """
    Log a fatal QQError that cannot be recorded in the info file, then exit.

    This function is used when even the failure state cannot be persisted to
    the job info file (e.g., if the info file path is missing or corrupted).

    Args:
        exception (QQError): The error to log.

    Raises:
        SystemExit: Exits with the provided exit code.
    """
    logger.error(f"Fatal qq run error: {exception}")
    logger.error(
        "Failure state could not be logged into the job info file. Consider the job to be in an INCONSISTENT state!"
    )
    sys.exit(exit_code)


def _log_fatal_unexpected_error(exception: Exception, exit_code: int) -> NoReturn:
    """
    Log a fatal unexpected error that cannot be recorded in the info file, then exit.

    This function is called when an unforeseen exception occurs and even
    logging to the job info file fails. This indicates a bug in the program.

    Args:
        exception (Exception): The unexpected error to log.

    Raises:
        SystemExit: Exits with the provided exit code.
    """
    logger.error("Fatal qq run error!")
    logger.error(
        "Failure state could not be logged into the job info file. Consider the job to be in an INCONSISTENT state!"
    )
    logger.critical(exception, exc_info=True, stack_info=True)
    sys.exit(exit_code)
