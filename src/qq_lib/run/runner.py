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
from time import sleep
from types import FrameType
from typing import NoReturn

import qq_lib
from qq_lib.archive.archiver import Archiver
from qq_lib.batch.interface.meta import BatchMeta
from qq_lib.core.config import CFG
from qq_lib.core.error import (
    QQError,
    QQJobMismatchError,
    QQRunCommunicationError,
    QQRunFatalError,
)
from qq_lib.core.logger import get_logger
from qq_lib.core.retryer import Retryer
from qq_lib.info.informer import Informer
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

    def __init__(self, info_file: Path, host: str):
        """
        Initialize a new QQRunner instance.

        Args:
            info_file (Path): Path to the qq info file that contains job metadata.
            host (str): The hostname of the input machine from which the job was submitted.

        Raises:
            QQRunFatalError: If loading the QQ info file fails fatally during initialization.
        """
        # install a signal handler
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        # process running the wrapped script
        self._process: subprocess.Popen[str] | None = None

        self._info_file = Path(info_file)
        logger.debug(f"Info file: '{self._info_file}'.")

        self._input_machine = host
        logger.debug(f"Input machine: '{self._input_machine}'.")

        # load the info file or raise a fatal qq error if this fails
        try:
            # get the batch system from the environment variable (or guess it)
            self._batch_system = BatchMeta.fromEnvVarOrGuess()
            logger.debug(f"Batch system: {str(self._batch_system)}.")

            # get the id of the job from the batch system
            if not (job_id := self._batch_system.getJobId()):
                raise QQError("Job has no associated job id")

            # load the info file
            self._informer: Informer = Retryer(
                Informer.fromFile,
                self._info_file,
                host=self._input_machine,
                max_tries=CFG.runner.retry_tries,
                wait_seconds=CFG.runner.retry_wait,
            ).run()

            # check that the id of this job matches the job id in the info file
            if not self._informer.matchesJob(job_id):
                raise QQJobMismatchError(
                    "Info file does not correspond to the current job"
                )

            # check that the batch system in info file matches the one loaded from the environment variable
            if self._batch_system != self._informer.batch_system:
                raise QQError(
                    f"Batch system mismatch - env var: '{str(self._batch_system)}', info file: '{self._informer.batch_system}'"
                )

        except Exception as e:
            raise QQRunFatalError(
                f"Unable to load valid qq info file '{self._info_file}' on '{self._input_machine}': {e}"
            ) from e

        logger.info(
            f"[qq-{str(self._batch_system)} v{qq_lib.__version__}] Initializing "
            f"job '{self._informer.info.job_id}' on host '{socket.gethostname()}'."
        )

        # get input directory
        self._input_dir = Path(self._informer.info.input_dir)
        logger.debug(f"Input directory: {self._input_dir}.")

        # should the scratch directory be used?
        self._use_scratch = self._informer.usesScratch()
        logger.debug(f"Use scratch: {self._use_scratch}.")

        # initialize archiver, if this is a loop job
        if loop_info := self._informer.info.loop_info:
            self._archiver = Archiver(
                loop_info.archive,
                loop_info.archive_format,
                self._informer.info.input_machine,
                self._informer.info.input_dir,
                self._batch_system,
            )
            self._should_resubmit = True
        else:
            self._archiver = None

    def prepare(self) -> None:
        """
        Prepare the script for execution, setting up the archive
        and archiving run time files (if this is a loop job) and
        preparing working directory.

        Raises:
            QQError: If working directory setup fails.
        """
        if self._archiver:
            assert self._informer.info.loop_info is not None
            # prepare the directory for archiving
            self._archiver.makeArchiveDir()

            # archive runtime files from the previous cycle
            # this has to be done before the working directory is prepared,
            # otherwise the runtime files would get copied to the working directory
            logger.debug(
                f"Archiving run time files from cycle {self._informer.info.loop_info.current - 1}."
            )
            self._archiver.archiveRunTimeFiles(
                # we need to escape the '+' character
                f"{self._informer.info.script_name}{CFG.loop_jobs.pattern.replace('+', '\\+') % (self._informer.info.loop_info.current - 1)}",
                self._informer.info.loop_info.current - 1,
            )

        if self._use_scratch:
            self._setUpScratchDir()
        else:
            self._setUpSharedDir()

        if self._archiver:
            assert self._informer.info.loop_info is not None
            # fetch files for the current cycle of the loop job from the archive
            self._archiver.fromArchive(
                self._work_dir, self._informer.info.loop_info.current
            )

    def execute(self) -> int:
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

        try:
            with Path(stdout_log).open("w") as out, Path(stderr_log).open("w") as err:
                self._process = subprocess.Popen(
                    ["bash", str(script)],
                    stdout=out,
                    stderr=err,
                    text=True,
                )

                # wait for the process to finish in a non-blocking manner
                while self._process.poll() is None:
                    sleep(CFG.runner.subprocess_checks_wait_time)

        except Exception as e:
            raise QQError(f"Failed to execute script '{script}': {e}") from e

        # if the script returns an exit code corresponding to CFG.exit_codes.qq_run_no_resubmit,
        # do not submit the next cycle of the job but return 0
        if (
            self._informer.info.loop_info is not None
            and self._process.returncode == CFG.exit_codes.qq_run_no_resubmit
        ):
            logger.debug(
                f"Detected an exit code of '{self._process.returncode}'. Replacing with '0' and will not submit the next cycle of the job."
            )
            self._process.returncode = 0
            self._should_resubmit = False

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
        assert self._process is not None

        if self._process.returncode == 0:
            # archive files
            if self._archiver:
                self._archiver.toArchive(self._work_dir)

            if self._use_scratch:
                # copy files back to the input (submission) directory
                Retryer(
                    self._batch_system.syncWithExclusions,
                    self._work_dir,
                    self._input_dir,
                    socket.gethostname(),
                    self._informer.info.input_machine,
                    max_tries=CFG.runner.retry_tries,
                    wait_seconds=CFG.runner.retry_wait,
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

    def logFailureAndExit(self, exception: BaseException) -> NoReturn:
        """
        Record a failure state into the qq info file and exit the program.

        Args:
            exception (BaseException): The exception to log.

        Raises:
            SystemExit: Always exits with the exit code associated with the given exception.
        """
        exit_code = getattr(exception, "exit_code", CFG.exit_codes.unexpected_error)
        try:
            self._updateInfoFailed(exit_code)
            logger.error(exception)
            sys.exit(exit_code)
        except Exception as e:
            # unable to log the current state into the info file
            log_fatal_error_and_exit(e)  # exits here

    def _setUpSharedDir(self) -> None:
        """
        Configure the input directory as the working directory.
        """
        # set qq working directory to the input dir
        self._work_dir = self._input_dir

        # move to the working directory
        Retryer(
            os.chdir,
            self._work_dir,
            max_tries=CFG.runner.retry_tries,
            wait_seconds=CFG.runner.retry_wait,
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
        self._work_dir = (scratch_dir / CFG.runner.scratch_dir_inner).resolve()
        logger.info(f"Setting up working directory in '{self._work_dir}'.")
        Retryer(
            Path.mkdir,
            self._work_dir,
            max_tries=CFG.runner.retry_tries,
            wait_seconds=CFG.runner.retry_wait,
        ).run()

        # move to the working directory
        Retryer(
            os.chdir,
            self._work_dir,
            max_tries=CFG.runner.retry_tries,
            wait_seconds=CFG.runner.retry_wait,
        ).run()

        # files excluded from copying to the working directory
        excluded = self._informer.info.excluded_files + [self._info_file]
        if self._archiver:
            excluded.append(self._archiver._archive)

        # copy files to the working directory
        Retryer(
            self._batch_system.syncWithExclusions,
            self._input_dir,
            self._work_dir,
            self._informer.info.input_machine,
            socket.gethostname(),
            excluded,
            max_tries=CFG.runner.retry_tries,
            wait_seconds=CFG.runner.retry_wait,
        ).run()

    def _deleteWorkDir(self) -> None:
        """
        Delete the entire working directory.

        Used only after successful execution in scratch space.
        """
        logger.debug(f"Removing working directory '{self._work_dir}'.")
        Retryer(
            shutil.rmtree,
            self._work_dir,
            max_tries=CFG.runner.retry_tries,
            wait_seconds=CFG.runner.retry_wait,
        ).run()

    def _updateInfoRunning(self) -> None:
        """
        Update the qq info file to mark the job as running.

        Raises:
            QQRunCommunicationError: If the job was killed without informing QQRunner.
            QQError: If the info file cannot be updated.
        """
        logger.debug(f"Updating '{self._info_file}' at job start.")
        self._reloadInfoAndEnsureValid()

        try:
            nodes = self._informer.getNodes()
            if not nodes:
                raise QQError(
                    "Could not get the list of used nodes from the batch server"
                )

            self._informer.setRunning(
                datetime.now(),
                socket.gethostname(),
                nodes,
                self._work_dir,
            )

            Retryer(
                self._informer.toFile,
                self._info_file,
                host=self._input_machine,
                max_tries=CFG.runner.retry_tries,
                wait_seconds=CFG.runner.retry_wait,
            ).run()
        except Exception as e:
            raise QQError(
                f"Could not update qqinfo file '{self._info_file}' at JOB START: {e}."
            ) from e

    def _updateInfoFinished(self) -> None:
        """
        Update the qq info file to mark the job as successfully finished.

        Logs errors as warnings if updating fails.

        Raises:
            QQRunCommunicationError: If the job was killed without informing QQRunner.
        """
        logger.debug(f"Updating '{self._info_file}' at job completion.")
        self._reloadInfoAndEnsureValid()

        try:
            self._informer.setFinished(datetime.now())
            Retryer(
                self._informer.toFile,
                self._info_file,
                host=self._input_machine,
                max_tries=CFG.runner.retry_tries,
                wait_seconds=CFG.runner.retry_wait,
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

        Raises:
            QQRunCommunicationError: If the job was killed without informing QQRunner.
        """
        logger.debug(f"Updating '{self._info_file}' at job failure.")
        self._reloadInfoAndEnsureValid()

        try:
            self._informer.setFailed(datetime.now(), return_code)
            Retryer(
                self._informer.toFile,
                self._info_file,
                host=self._input_machine,
                max_tries=CFG.runner.retry_tries,
                wait_seconds=CFG.runner.retry_wait,
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
        self._reloadInfoAndEnsureValid(retry=False)

        try:
            self._informer.setKilled(datetime.now())
            # no retrying here since we cannot affort multiple attempts here
            self._informer.toFile(self._info_file, host=self._input_machine)
        except Exception as e:
            logger.warning(
                f"Could not update qqinfo file '{self._info_file}' at JOB KILL: {e}."
            )

    def _reloadInfo(self, retry: bool = True) -> None:
        """
        Reload the qq job info file for this job.

        Args:
            retry (bool): Retry the loading operation if it fails.

        Raises:
            QQError: If the qq info file cannot be reach or read after retrying.
        """
        if retry:
            self._informer = Retryer(
                Informer.fromFile,
                self._info_file,
                host=self._input_machine,
                max_tries=CFG.runner.retry_tries,
                wait_seconds=CFG.runner.retry_wait,
            ).run()
        else:
            self._informer = Informer.fromFile(self._info_file, self._input_machine)

    def _ensureMatchesJob(self, job_id: str) -> None:
        """
        Ensure that the provided job_id matches the job id in the wrapped informer.

        Raises:
            QQJobMismatchError: If the info file corresponds to a different job.
        """
        if not self._informer.matchesJob(job_id):
            raise QQJobMismatchError(
                f"Info file '{self._info_file}' does not correspond to job '{job_id}'."
            )

    def _ensureNotKilled(self) -> None:
        """
        Ensure that the job has not been killed.

        Raises:
            QQRunCommunicationError: If the job state is `KILLED`.
        """
        if self._informer.info.job_state == NaiveState.KILLED:
            raise QQRunCommunicationError(
                "Job has been killed without informing qq run. Aborting the job!"
            )

    def _reloadInfoAndEnsureValid(self, retry: bool = False) -> None:
        """
        Reload the qq job info file and check that it corresponds to the current job
        by comparing job ids.

        Then check the job's state and ensure it is not killed.

        Args:
            retry (bool): Retry the loading operation if it fails.

        Raises:
            QQJobMismatchError: If the info file corresponds to a different job.
            QQRunCommunicationError: If the job state is `KILLED`.
            QQError: If the qq info file cannot be reached or read.
        """
        job_id = self._informer.info.job_id
        self._reloadInfo(retry)
        self._ensureMatchesJob(job_id)
        self._ensureNotKilled()

    def _resubmit(self) -> None:
        """
        Resubmit the current loop job to the batch system if additional cycles remain.

        Raises:
            QQError: If the job cannot be resubmitted.
        """
        if not (loop_info := self._informer.info.loop_info):
            logger.debug("Loop info is undefined while resubmiting. This is a bug!")
            return

        if loop_info.current >= loop_info.end:
            logger.info("This was the final cycle of the loop job. Not resubmitting.")
            return

        if not self._should_resubmit:
            logger.info(
                f"The script finished with an exit code of '{CFG.exit_codes.qq_run_no_resubmit}' indicating that the next cycle of the job should not be submitted. Not resubmitting."
            )
            return

        logger.info("Resubmitting the job.")
        logger.debug(
            f"Resubmitting using the batch system '{str(self._batch_system)}'."
        )

        Retryer(
            self._batch_system.resubmit,
            input_machine=self._informer.info.input_machine,
            input_dir=self._informer.info.input_dir,
            command_line=self._prepareCommandLineForResubmit(),
            max_tries=CFG.runner.retry_tries,
            wait_seconds=CFG.runner.retry_wait,
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

        - Marks job as killed in the info file.
        - Terminates the subprocess.
        """
        # update the qq info file
        self._updateInfoKilled()

        # send SIGTERM to the running process, if there is any
        # this may potentially not even be called -- the subprocess might be already terminated
        if self._process and self._process.poll() is None:
            logger.info("Cleaning up: terminating subprocess.")
            self._process.terminate()

            # wait for the subprocess to exit, then SIGKILL it
            sleep(CFG.runner.sigterm_to_sigkill)
            if self._process and self._process.poll() is None:
                self._process.kill()

    def _handle_sigterm(self, _signum: int, _frame: FrameType | None) -> NoReturn:
        """
        Signal handler for SIGTERM.

        Performs cleanup, logs termination, and exits.
        """
        logger.info("Received SIGTERM, initiating shutdown.")
        self._cleanup()
        logger.error("Execution was terminated by SIGTERM.")
        # this may get ignored by the batch system
        # so you should not rely on this specific exit code
        sys.exit(143)


def log_fatal_error_and_exit(exception: BaseException) -> NoReturn:
    """
    Log an error that cannot be recorded in the info file, then exit.

    This function is used when even the failure state cannot be persisted to
    the job info file (e.g., if the info file path is missing or corrupted).

    Args:
        exception (BaseException): The error to log.

    Raises:
        SystemExit: Exits with an exit code associated with the exception.
    """
    logger.error(f"Fatal qq run error: {exception}")
    logger.error("Failure state was NOT logged into the job info file.")

    if isinstance(exception, (QQRunFatalError, QQRunCommunicationError, QQError)):
        sys.exit(exception.exit_code)

    logger.critical(exception, exc_info=True, stack_info=True)
    sys.exit(CFG.exit_codes.unexpected_error)
