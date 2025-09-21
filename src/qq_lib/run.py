# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

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
SIGTERM_TO_SIGKILL = 10

logger = get_logger(__name__)


@click.command(hidden=True)
@click.argument("script_path", type=str)
def run(script_path: str):
    """
    Execute a script within the qq environment.
    """
    guard()
    try:
        runner = QQRunner()
    except QQError as e:
        # the most basic setup of the run failed
        # can't even log the failure state to the info file
        _log_fatal_qq_error(e)
    except Exception as e:
        _log_fatal_unexpected_error(e)

    try:
        runner.setUp(Path(script_path))
        runner.setUpWorkDir()
        exit_code = runner.executeScript()
        sys.exit(exit_code)
    except QQError as e:
        # if the execution fails, log this error into both stderr and the info file
        logger.error(e)
        runner.logFailureIntoInfoFile()
    except Exception as e:
        # even unknown exceptions should be logged into both stderr and the info file
        # this indicates a bug in the program
        logger.critical(e, exc_info=True, stack_info=True)
        runner.logFailureIntoInfoFile()
        sys.exit(99)


class QQRunner:
    """
    Handles the setup and execution of scripts within the qq batch environment.
    """

    def __init__(self):
        """
        Initialize the QQRunner.
        This performs only the most basic set-up. If this fails, a fatal error is raised
        without logging it into the qq info file.
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

    def setUp(self, script: Path):
        """
        Perform proper set up of the QQRunner that can fail cleanly.
        """
        # get job directory
        job_dir = os.environ.get(JOBDIR)
        if not job_dir:
            raise QQError(f"'{JOBDIR}' environment variable is not set.")
        logger.debug(f"Job directory: '{job_dir}'.")

        self.job_dir = Path(job_dir)

        # check that the script to run exists
        self.script = script
        if not self.script.is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

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
        if self.use_scratch:
            self._setUpScratchDir()
        else:
            self._setUpSharedDir()

    def executeScript(self) -> int:
        """
        Execute the script associated with this runner in the working directory.
        Skips the shebang line of the script.

        Returns:
            int: The return code from the script execution.

        Raises:
            QQError: If execution of the script fails.
        """
        # update the qqinfo file
        self._updateInfoRun()

        # get paths to output files
        stdout_log = os.environ.get(STDOUT_FILE)
        stderr_log = os.environ.get(STDERR_FILE)
        if not stdout_log or not stderr_log:
            raise QQError(
                f"'{STDOUT_FILE}' or '{STDERR_FILE}' environment variable has not been set up."
            )

        with self.script.open() as file:
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
            raise QQError(f"Failed to execute script '{self.script}': {e}") from e

        if self.use_scratch:
            # copy files back to the submission directory
            self._copyFilesFromWorkDir()
        else:
            # if on shared storage, remove the temporary submitted script
            self._removeScript()

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

    def logFailureIntoInfoFile(self):
        try:
            self._updateInfoFailed(91)
            sys.exit(91)
        except QQError as e:
            _log_fatal_qq_error(e)
        except Exception as e:
            _log_fatal_unexpected_error(e)

    def _setUpSharedDir(self):
        # set qq working directory to job directory
        self.work_dir = self.job_dir

        # export working directory path as an environment variable
        os.environ[WORKDIR] = str(self.work_dir)

        # copy the executed script to the working directory
        self._copyScriptToDir(self.work_dir)

        # move to the working directory
        os.chdir(self.work_dir)

    def _setUpScratchDir(self):
        # set qq working directory to scratch directory
        work_dir = os.environ.get(self.batch_system.scratchDirEnvVar())
        if not work_dir:
            raise QQError(
                f"Could not get the scratch directory from '{self.batch_system.scratchDirEnvVar()}' environment variable."
            )
        self.work_dir = Path(work_dir)

        # export working directory path as an environment variable
        os.environ[WORKDIR] = str(self.work_dir)

        # copy the executed script to the working directory
        self._copyScriptToDir(self.work_dir)

        # move to the working directory
        os.chdir(self.work_dir)

        # copy files to the working directory excluding the qq info file
        self._copyFilesToDst(
            self._getFilesToCopy(self.job_dir, [self.info_file]), self.work_dir
        )

    def _copyScriptToDir(self, directory: Path):
        """
        Copy a script file into a target directory.
        Reassings the `self.script` path to the script in the target directory.

        Args:
            script (Path): The script file to copy, relative to the batch system's (not qq's) working directory.
            directory (Path): The destination directory where the script will be copied.

        Raises:
            QQError: If the script cannot be copied to the destination directory.
        """
        try:
            self.script = Path(
                shutil.copy2(
                    # copy from the batch system's default working directory
                    Path(self.batch_system.workDirEnvVar()) / self.script,
                    directory,
                )
            )
        except Exception as e:
            raise QQError(
                f"Could not copy '{self.script}' into directory '{directory}': {e}."
            ) from e

    def _copyFilesToDst(self, src: Sequence[Path], directory: Path):
        """
        Copy multiple files and directories into a target directory.

        Args:
            src (Sequence[Path]): A sequence of file/directory paths to copy.
            directory (Path): The destination directory where items will be copied.

        Raises:
            QQError: If any item cannot be copied to the destination directory.
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
        Copy files from workdir back to jobdir.
        """
        logger.debug(f"Copying files from {self.work_dir} to {self.job_dir}.")

        # copy everything except for the executed script
        files_to_copy = self._getFilesToCopy(self.work_dir, [self.script])
        logger.debug(f"Copying the following paths: {files_to_copy}.")
        self._copyFilesToDst(files_to_copy, self.job_dir)

    def _removeFilesFromWorkDir(self):
        for item in self.work_dir.iterdir():
            if item.is_file() or item.is_symlink():
                logger.debug(f"Removing file {item}.")
                item.unlink()
            elif item.is_dir():
                logger.debug(f"Removing directory {item}.")
                shutil.rmtree(item)

    def _removeScript(self):
        try:
            Path.unlink(self.script, missing_ok=True)
        except Exception as e:
            logger.warning(
                f"Could not delete the temporary run script '{self.script}': {e}"
            )

    def _getFilesToCopy(
        self, directory: Path, filter_out: list[str] | None = None
    ) -> list[Path]:
        """
        Get the list of files in a directory that should be copied, optionally filtering out some files.

        Args:
            directory (Path): The source directory to scan for files.
            filter_out (Optional[List[str]]): A list of file or directory paths to exclude from the result.

        Returns:
            List[Path]: A list of absolute Path objects for files and directories in `directory`
                        that are not in the `filter_out` list.
        """
        root = Path(directory).resolve()
        # normalize filter_out to absolute paths
        filter_paths = {Path(p).resolve() for p in filter_out} if filter_out else set()
        logger.debug(f"Paths to filter out: {filter_paths}.")

        return [p for p in root.iterdir() if p.resolve() not in filter_paths]

    def _updateInfoRun(self):
        logger.debug(f"Updating '{self.info_file}' at job start.")
        try:
            self.info.setRunning(datetime.now(), socket.gethostname(), self.work_dir)
            self.info.exportToFile(self.info_file)
        except Exception as e:
            raise QQError(
                f"Could not update qqinfo file '{self.info_file}' at JOB START: {e}."
            ) from e

    def _updateInfoFinished(self):
        logger.debug(f"Updating '{self.info_file}' at job completion.")
        try:
            self.info.setFinished(datetime.now())
            self.info.exportToFile(self.info_file)
        except Exception as e:
            logger.warning(
                f"Could not update qqinfo file '{self.info_file}' at JOB COMPLETION: {e}."
            )

    def _updateInfoFailed(self, return_code: int):
        logger.debug(f"Updating '{self.info_file}' at job failure.")
        try:
            self.info.setFailed(datetime.now(), return_code)
            self.info.exportToFile(self.info_file)
        except Exception as e:
            logger.warning(
                f"Could not update qqinfo file '{self.info_file}' at JOB FAILURE: {e}."
            )

    def _updateInfoKilled(self):
        logger.debug(f"Updating '{self.info_file}' at job kill.")
        try:
            self.info.setKilled(datetime.now())
            self.info.exportToFile(self.info_file)
        except Exception as e:
            logger.warning(
                f"Could not update qqinfo file '{self.info_file}' at JOB KILL: {e}."
            )

    def _cleanup(self) -> None:
        """Perform clean-up of the execution in case of SIGTERM or an exception from the subprocess."""
        # update the qq info file
        self._updateInfoKilled()
        # remove the temporary script file, if in shared storage
        if not self.use_scratch:
            logger.debug("Removing temporary run script.")
            self._removeScript()

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
        """Signal handler for SIGTERM."""
        logger.info("Received SIGTERM, initiating shutdown.")
        self._cleanup()
        logger.error("Execution was terminated by SIGTERM.")
        sys.exit(15)


def _log_fatal_qq_error(exception: QQError):
    """
    For logging fatal qq errors. This should be used if logging of a QQError into an info file fails.

    Exits the program with error code 92.
    """
    logger.error(f"Fatal qq run error: {exception}")
    logger.error(
        "Failure state could not be logged into the job info file. Consider the job to be in an INCONSISTENT state!"
    )
    sys.exit(92)


def _log_fatal_unexpected_error(exception: Exception):
    """
    For logging fatal unexpected errors. This should be used if logging of another error into an info file fails.

    Exits the program with error code 99.
    """
    logger.error("Fatal qq run error!")
    logger.error(
        "Failure state could not be logged into the job info file. Consider the job to be in an INCONSISTENT state!"
    )
    logger.critical(exception, exc_info=True, stack_info=True)
    sys.exit(99)
