# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import shutil
import stat
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import click

from qq_lib.base import QQBatchInterface
from qq_lib.env_vars import JOBDIR, STDERR_FILE, STDOUT_FILE, WORKDIR
from qq_lib.error import QQError
from qq_lib.guard import guard
from qq_lib.logger import get_logger
from qq_lib.pbs import QQPBS

logger = get_logger("qq run")


@click.command()
@click.argument("script_path", type=str)
def run(script_path: str):
    """
    Execute a script within the qq environment.
    """
    try:
        guard()
        runner = QQRunner(QQPBS, script_path)
        runner.setUpWorkDir()
        sys.exit(runner.executeScript())
    except Exception as e:
        logger.error(e)
        sys.exit(1)


class QQRunner:
    """
    Handles the setup and execution of scripts within the qq batch environment.

    Attributes:
        SCRATCH_DIRS (List[Path]): Ordered list of supported scratch directories.
        batch_system (Type[QQBatchInterface]): The batch system interface.
        username (str): Username retrieved from environment variable.
        jobid (str): Job ID retrieved from environment variable.
    """

    # Supported scratch directories. Directories are in order of decreasing preference.
    SCRATCH_DIRS = [Path("/scratch.ssd"), Path("/scratch")]

    def __init__(self, batch_system: type[QQBatchInterface], script: str):
        """
        Initialize the QQRunner with the specified batch system.

        Args:
            batch_system (Type[QQBatchInterface]): The batch system interface.
            script (str): Name of the script to be executed.

        Raises:
            QQError: If required environment variables are not set.
            QQError: If the script does not exist or is not a file.
        """
        self.batch_system = batch_system
        self.script = Path(script)

        self.username = os.environ.get(self.batch_system.usernameEnvVar())
        self.jobid = os.environ.get(self.batch_system.jobIdEnvVar())

        if not self.username or not self.jobid:
            raise QQError(
                f"Required {self.batch_system.envName()} environment variables are not set."
            )

        if not self.script.is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

    def setUpWorkDir(self):
        """
        Set up a working directory for the current job.

        This method selects an appropriate scratch directory, ensures it is writable,
        creates a job-specific subdirectory, copies the necessary files into it, moves into it
        and sets the 'QQ_WORKDIR' environment variable.

        Raises:
            QQError: If no suitable scratch directory is found or directory creation fails.
        """
        scratch_dir = self._getScratchDir()
        if scratch_dir is None:
            raise QQError("Could not find a suitable scratch directory.")

        self._ensureWritable(scratch_dir)
        work_dir = scratch_dir / f"job_{self.jobid}"
        self._createWorkDir(work_dir)

        self._copyScriptToDir(work_dir)
        os.chdir(work_dir)

        job_dir = os.environ.get(JOBDIR)
        if job_dir is None:
            raise QQError(f"'{JOBDIR}' environment variable is not set.")

        self._copyFilesToDst(self._getFilesToCopy(job_dir), work_dir)

        os.environ[WORKDIR] = str(work_dir)

    def executeScript(self) -> int:
        """
        Execute the script associated with this runner in the working directory.
        Skips the shebang line of the script.

        Returns:
            int: The return code from the script execution.

        Raises:
            QQError: If execution of the script fails.
        """
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
            with open(stdout_log, "w") as out, open(stderr_log, "w") as err:
                process = subprocess.Popen(
                    ["bash"],
                    stdin=subprocess.PIPE,
                    stdout=out,
                    stderr=err,
                    text=True,
                )
                process.communicate(input="".join(lines))

        except Exception as e:
            raise QQError(f"Failed to execute script '{self.script}': {e}") from e

        # copy files back to the submission directory
        self._copyFilesBack()

        return process.returncode

    def _getScratchDir(self) -> Path | None:
        """
        Find a suitable scratch directory for the user.

        Returns:
            Union[Path, None]: The path to an existing scratch directory or None if none are available.
        """
        for base_dir in self.SCRATCH_DIRS:
            scratch_dir = base_dir / self.username
            if scratch_dir.exists():
                return scratch_dir

    def _ensureWritable(self, directory: Path):
        """
        Ensure that the specified directory is writable.

        If the directory is not writable, attempts to set appropriate permissions.

        Args:
            directory (Path): Directory to check and make writable if needed.

        Raises:
            QQError: If write permissions cannot be set.
        """
        if not os.access(directory, os.W_OK):
            logger.info(f"'{directory}' is not writable. Will set permissions.")

            try:
                directory.chmod(directory.stat().st_mode | stat.S_IWUSR)
            except Exception as e:
                raise QQError(
                    f"Could not set write permissions for '{directory}': {e}"
                ) from e

    def _createWorkDir(self, directory: Path):
        """
        Create a working directory for the job.

        Args:
            directory (Path): The path to the directory to create.

        Raises:
            QQError: If the directory cannot be created.
        """
        try:
            directory.mkdir(parents=False, exist_ok=False)
        except Exception as e:
            raise QQError(
                f"Could not create working directory '{directory}': {e}"
            ) from e

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
                    Path(self.batch_system.workDirEnvVar()) / self.script, directory
                )
            )
        except Exception as e:
            raise QQError(
                f"Could not copy '{self.script}' into directory '{directory}': {e}"
            ) from e

    def _copyFilesToDst(self, src: Sequence[Path], directory: Path):
        """
        Copy multiple files into a target directory.

        Args:
            src (Sequence[Path]): A sequence of file paths to copy.
            directory (Path): The destination directory where files will be copied.

        Raises:
            QQError: If any file cannot be copied to the destination directory.
        """
        for file in src:
            try:
                shutil.copy2(file, directory)
            except Exception as e:
                raise QQError(
                    f"Could not copy '{file}' into directory '{directory}': {e}"
                ) from e

    def _copyFilesBack(self):
        """
        Copy files from workdir back to jobdir.
        """
        work_dir = os.environ.get(WORKDIR)
        job_dir = os.environ.get(JOBDIR)
        if not work_dir or not job_dir:
            raise QQError(
                f"Environment variables '{WORKDIR}' or '{JOBDIR}' are not properly set."
            )

        logger.debug(f"Copying files from {work_dir} to {job_dir}.")

        # copy everything except for the executed script
        files_to_copy = self._getFilesToCopy(work_dir, [self.script])
        logger.debug(f"Copying the following paths: {files_to_copy}")
        self._copyFilesToDst(files_to_copy, job_dir)

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
        logger.debug(f"Paths to filter out: {filter_paths}")

        return [p for p in root.iterdir() if p.resolve() not in filter_paths]
