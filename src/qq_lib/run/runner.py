# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import shutil
import signal
import socket
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import NoReturn

import qq_lib
from qq_lib.archive.archiver import QQArchiver
from qq_lib.core.constants import (
    INFO_FILE,
    INPUT_MACHINE,
    LOOP_JOB_PATTERN,
    RUNNER_RETRY_TRIES,
    RUNNER_RETRY_WAIT,
    RUNNER_SIGTERM_TO_SIGKILL,
    SCRATCH_DIR_INNER,
)
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.core.retryer import QQRetryer
from qq_lib.info.informer import QQInformer
from qq_lib.properties.job_type import QQJobType
from qq_lib.properties.states import NaiveState

logger = get_logger(__name__, show_time=True)


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
        # process running the wrapped script
        self._process: subprocess.Popen[str] | None = None

        # object used for archiving data
        self._archiver = None

        # install a signal handler
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        # get path to the qq info file
        info_file_string = os.environ.get(INFO_FILE)
        if not info_file_string:
            raise QQError(f"'{INFO_FILE}' environment variable is not set.")
        self._info_file = Path(info_file_string)
        logger.debug(f"Info file: '{self._info_file}'.")

        # get input machine
        if not (machine := os.environ.get(INPUT_MACHINE)):
            raise QQError(f"'{INPUT_MACHINE}' environment variable is not set.")
        self._input_machine = machine
        logger.debug(f"Input machine: '{self._input_machine}'.")

        # load the info file
        self._informer: QQInformer = QQRetryer(
            QQInformer.fromFile,
            self._info_file,
            host=self._input_machine,
            max_tries=RUNNER_RETRY_TRIES,
            wait_seconds=RUNNER_RETRY_WAIT,
        ).run()

    def setUp(self) -> None:
        """
        Perform the full setup of the QQRunner.

        Raises:
            QQError: If required environment variables are missing or invalid.
        """
        logger.info(
            f"[{str(self._informer.batch_system)}-qq v{qq_lib.__version__}] Initializing "
            f"job '{self._informer.info.job_id}' on host '{socket.gethostname()}'."
        )

        # get input directory
        self._input_dir = Path(self._informer.info.input_dir)
        logger.debug(f"Input directory: {self._input_dir}.")

        # get the batch system
        self._batch_system = self._informer.batch_system
        logger.debug(f"Batch system: {str(self._batch_system)}.")

        # should the scratch directory be used?
        self._use_scratch = self._informer.usesScratch()
        logger.debug(f"Use scratch: {self._use_scratch}.")

        # initialize archiver, if this is a loop job
        if loop_info := self._informer.info.loop_info:
            self._archiver = QQArchiver(
                loop_info.archive,
                loop_info.archive_format,
                self._informer.info.input_machine,
                self._informer.info.input_dir,
                self._batch_system,
            )

            self._archiver.makeArchiveDir()
            # archive run time files from the previous cycle
            logger.debug(
                f"Archiving run time files from cycle {loop_info.current - 1}."
            )
            self._archiver.archiveRunTimeFiles(
                # we need to escape the '+' character
                f"{self._informer.info.script_name}{LOOP_JOB_PATTERN.replace('+', '\\+') % (loop_info.current - 1)}",
                loop_info.current - 1,
            )

    def setUpWorkDir(self) -> None:
        """
        Prepare the working directory for job execution.

        Depending on configuration, this sets up:
          - A scratch directory (copying job files there), or
          - The shared job directory directly.

        Raises:
            QQError: If working directory setup fails.
        """
        if self._use_scratch:
            self._setUpScratchDir()
        else:
            self._setUpSharedDir()

        if self._archiver:
            self._archiver.archiveFrom(
                self._work_dir, self._informer.info.loop_info.current
            )

    def executeScript(self) -> int:
        """
        Execute the job script in the working directory.

        Returns:
            int: The exit code from the executed script.

        Raises:
            QQError: If execution fails or info file cannot be updated.
        """
        # update the qqinfo file
        self._updateInfoRunning()

        # get the actual name of the script to execute
        script = Path(self._informer.info.script_name).resolve()

        # get paths to output files
        stdout_log = self._informer.info.stdout_file
        stderr_log = self._informer.info.stderr_file

        logger.info(f"Executing script '{script}'.")
        with script.open() as file:
            lines = file.readlines()[1:]

        try:
            with Path.open(stdout_log, "w") as out, Path.open(stderr_log, "w") as err:
                self._process = subprocess.Popen(
                    ["bash"],
                    stdin=subprocess.PIPE,
                    stdout=out,
                    stderr=err,
                    text=True,
                )
                self._process.communicate(input="".join(lines))

        except Exception as e:
            raise QQError(f"Failed to execute script '{script}': {e}") from e

        return self._process.returncode

    def finalize(self) -> None:
        """
        Finalize the execution of the job script.

        This method handles post-processing depending on the success or failure
        of the script:

        - On success (process return code 0):
            - Updates the qq info file to indicate the job is "finished".
            - If `use_scratch` is True, copies job files back from the scratch
            directory to the submission directory and removes them from scratch.

        - On failure (non-zero return code):
            - Updates the qq info file to indicate the job "failed".
            - If `use_scratch` is True, files remain in the scratch directory
            for debugging purposes.

        Raises:
            QQError: If copying or deletion of files fails.
        """
        logger.info("Finalizing the execution.")

        if self._process.returncode == 0:
            # archive files
            if self._archiver:
                self._archiver.archiveTo(self._work_dir)

            if self._use_scratch:
                # copy files back to the input (submission) directory
                QQRetryer(
                    self._batch_system.syncWithExclusions,
                    self._work_dir,
                    self._input_dir,
                    socket.gethostname(),
                    self._informer.info.input_machine,
                    max_tries=RUNNER_RETRY_TRIES,
                    wait_seconds=RUNNER_RETRY_WAIT,
                ).run()

                # remove the working directory from scratch
                # directory is retained on scratch if the run fails for any reason
                self._deleteWorkDir()

            # update the qqinfo file
            self._updateInfoFinished()

            # if this is a loop job
            if self._informer.info.job_type == QQJobType.LOOP:
                self._resubmit()
        else:
            # only update the qqinfo file
            self._updateInfoFailed(self._process.returncode)

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
                log_fatal_unexpected_error(e, exit_code)
            log_fatal_qq_error(e, exit_code)
        except Exception as e:
            log_fatal_unexpected_error(e, exit_code)

    def _setUpSharedDir(self) -> None:
        """
        Configure the input directory as the working directory.
        """
        # set qq working directory to the input dir
        self._work_dir = self._input_dir

        # move to the working directory
        QQRetryer(
            os.chdir,
            self._work_dir,
            max_tries=RUNNER_RETRY_TRIES,
            wait_seconds=RUNNER_RETRY_WAIT,
        ).run()

    def _setUpScratchDir(self) -> None:
        """
        Configure a scratch directory as the working directory.

        Copies all files from the job directory to the working directory
        (excluding the qq info file).

        Raises:
            QQError: If scratch directory cannot be determined.
        """
        # get scratch directory (this directory should be created and allocated by the batch system)
        scratch_dir = self._batch_system.getScratchDir(self._informer.info.job_id)

        # create working directory inside the scratch directory allocated by the batch system
        # we create this directory because other processes may write files
        # into the allocated scratch directory and we do not want these files
        # to affect the job execution or be copied back to input_dir
        # this also makes it easier to delete the working directory after completion
        self._work_dir = (scratch_dir / SCRATCH_DIR_INNER).resolve()
        logger.info(f"Setting up working directory in '{self._work_dir}'.")
        QQRetryer(
            Path.mkdir,
            self._work_dir,
            max_tries=RUNNER_RETRY_TRIES,
            wait_seconds=RUNNER_RETRY_WAIT,
        ).run()

        # move to the working directory
        QQRetryer(
            os.chdir,
            self._work_dir,
            max_tries=RUNNER_RETRY_TRIES,
            wait_seconds=RUNNER_RETRY_WAIT,
        ).run()

        # files excluded from copying to the working directory
        excluded = self._informer.info.excluded_files + [self._info_file]
        if self._archiver:
            excluded.append(self._archiver._archive)

        # copy files to the working directory
        QQRetryer(
            self._batch_system.syncWithExclusions,
            self._input_dir,
            self._work_dir,
            self._informer.info.input_machine,
            socket.gethostname(),
            excluded,
            max_tries=RUNNER_RETRY_TRIES,
            wait_seconds=RUNNER_RETRY_WAIT,
        ).run()

    def _deleteWorkDir(self) -> None:
        """
        Delete the entire working directory.

        Used only after successful execution in scratch space.
        """
        logger.debug(f"Removing working directory '{self._work_dir}'.")
        QQRetryer(
            shutil.rmtree,
            self._work_dir,
            max_tries=RUNNER_RETRY_TRIES,
            wait_seconds=RUNNER_RETRY_WAIT,
        ).run()

    def _updateInfoRunning(self) -> None:
        """
        Update the qq info file to mark the job as running.

        Raises:
            QQError: If the info file cannot be updated.
        """
        logger.debug(f"Updating '{self._info_file}' at job start.")
        try:
            self._reloadInfoAndCheckKill()
            self._informer.setRunning(
                datetime.now(),
                socket.gethostname(),
                self._informer.getNodes(),
                self._work_dir,
            )

            QQRetryer(
                self._informer.toFile,
                self._info_file,
                host=self._input_machine,
                max_tries=RUNNER_RETRY_TRIES,
                wait_seconds=RUNNER_RETRY_WAIT,
            ).run()
        except Exception as e:
            raise QQError(
                f"Could not update qqinfo file '{self._info_file}' at JOB START: {e}."
            ) from e

    def _updateInfoFinished(self) -> None:
        """
        Update the qq info file to mark the job as successfully finished.

        Logs errors as warnings if updating fails.
        """
        logger.debug(f"Updating '{self._info_file}' at job completion.")
        try:
            self._reloadInfoAndCheckKill()
            self._informer.setFinished(datetime.now())
            QQRetryer(
                self._informer.toFile,
                self._info_file,
                host=self._input_machine,
                max_tries=RUNNER_RETRY_TRIES,
                wait_seconds=RUNNER_RETRY_WAIT,
            ).run()
        except Exception as e:
            logger.warning(
                f"Could not update qqinfo file '{self._info_file}' at JOB COMPLETION: {e}."
            )

    def _updateInfoFailed(self, return_code: int) -> None:
        """
        Update the qq info file to mark the job as failed.

        Args:
            return_code (int): Exit code from the failed job.

        Logs errors as warnings if updating fails.
        """
        logger.debug(f"Updating '{self._info_file}' at job failure.")
        try:
            self._reloadInfoAndCheckKill()
            self._informer.setFailed(datetime.now(), return_code)
            QQRetryer(
                self._informer.toFile,
                self._info_file,
                host=self._input_machine,
                max_tries=RUNNER_RETRY_TRIES,
                wait_seconds=RUNNER_RETRY_WAIT,
            ).run()
        except Exception as e:
            logger.warning(
                f"Could not update qqinfo file '{self._info_file}' at JOB FAILURE: {e}."
            )

    def _updateInfoKilled(self) -> None:
        """
        Update the qq info file to mark the job as killed.

        Used during SIGTERM cleanup.

        Logs errors as warnings if updating fails.

        No retrying since there is no time for that.
        """
        logger.debug(f"Updating '{self._info_file}' at job kill.")
        try:
            self._informer.setKilled(datetime.now())
            # no retrying here since we cannot affort multiple attempts here
            self._informer.toFile(self._info_file, host=self._input_machine)
        except Exception as e:
            logger.warning(
                f"Could not update qqinfo file '{self._info_file}' at JOB KILL: {e}."
            )

    def _reloadInfoAndCheckKill(self) -> None:
        """
        Reload the qq job info file and check the job's state.

        If the job state is `KILLED`, the process exits immediately with code 93.

        Raises:
            QQError: If the qq info file cannot be reached or read.
        """
        self._informer = QQRetryer(
            QQInformer.fromFile,
            self._info_file,
            host=self._input_machine,
            max_tries=RUNNER_RETRY_TRIES,
            wait_seconds=RUNNER_RETRY_WAIT,
        ).run()

        if self._informer.info.job_state == NaiveState.KILLED:
            logger.error(
                "Job has been killed without informing qq run. Aborting the job!"
            )
            sys.exit(93)

    def _resubmit(self) -> None:
        """
        Resubmit the current loop job to the batch system if additional cycles remain.

        Raises:
            QQError: If the job cannot be resubmitted.
        """
        loop_info = self._informer.info.loop_info
        if loop_info.current >= loop_info.end:
            logger.info("This was the final cycle of the loop job. Not resubmitting.")
            return

        logger.info("Resubmitting the job.")

        QQRetryer(
            self._batch_system.resubmit,
            input_machine=self._informer.info.input_machine,
            input_dir=self._informer.info.input_dir,
            command_line=self._prepareCommandLineForResubmit(),
            max_tries=RUNNER_RETRY_TRIES,
            wait_seconds=RUNNER_RETRY_WAIT,
        ).run()

        logger.info("Job successfully resubmitted.")

    def _prepareCommandLineForResubmit(self) -> list[str]:
        """
        Prepare a modified command line for submitting the next cycle of a loop job.

        This method takes the original command line from the job's informer,
        removes any existing dependency options (i.e., arguments containing or
        following `"--depend"`), and appends a new dependency referencing the
        current job ID. This ensures that the resubmitted job depends on the
        successful completion (`afterok`) of the current one.

        Returns:
            line[str]: The sanitized and updated list of command line arguments.
        """
        command_line = self._informer.info.command_line

        # we need to remove dependencies for the previous cycle
        modified = []
        it = iter(command_line)
        for arg in it:
            if arg.strip() == "--depend":
                next(it, None)  # skip the following argument
            elif "--depend" not in arg:
                modified.append(arg)

        # and add in a new dependency for the current cycle
        modified.append(f"--depend=afterok={self._informer.info.job_id}")

        logger.debug(f"Command line for resubmit: {modified}.")
        return modified

    def _cleanup(self) -> None:
        """
        Clean up after execution is interrupted or killed.

        - Marks job as killed in the info file
        - Terminates the subprocess
        """
        # update the qq info file
        self._updateInfoKilled()

        # send SIGTERM to the running process, if there is any
        if self._process and self._process.poll() is None:
            logger.info("Cleaning up: terminating subprocess.")
            self._process.terminate()
            try:
                self._process.wait(timeout=RUNNER_SIGTERM_TO_SIGKILL)
            # kill the running process if this takes too long
            except subprocess.TimeoutExpired:
                logger.info("Subprocess did not exit, killing.")
                self._process.kill()

    def _handle_sigterm(self, _signum: int, _frame: FrameType | None) -> None:
        """
        Signal handler for SIGTERM.

        Performs cleanup, logs termination, and exits.
        """
        logger.info("Received SIGTERM, initiating shutdown.")
        self._cleanup()
        logger.error("Execution was terminated by SIGTERM.")
        # this may get ignored by the batch system
        # so you should not really on this exit code
        sys.exit(143)


def log_fatal_qq_error(exception: QQError, exit_code: int) -> NoReturn:
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


def log_fatal_unexpected_error(exception: Exception, exit_code: int) -> NoReturn:
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
