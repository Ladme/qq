# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import shutil
import stat
import subprocess
import sys
import click
import os
from pathlib import Path
from typing import List, Optional, Sequence, Type, Union

from qq_lib.env_vars import JOBDIR, WORKDIR
from qq_lib.guard import guard
from qq_lib.error import QQError
from qq_lib.pbs import QQPBS
from qq_lib.base import QQBatchInterface
from qq_lib.logger import get_logger

logger = get_logger("qq run")

@click.command()
@click.argument("script_path", type=str)
def run(script_path: str):
    """
    Execute a script within the qq environment.
    
    This function should not be called directly. Scripts must be executed
    by adding `qq run` to the script's shebang line.
    """
    try:
        guard()
        runner = QQRunner(QQPBS)
        runner.setUpWorkDir(script_path)
        sys.exit(runner.executeScript(script_path))
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

    def __init__(self, batch_system: Type[QQBatchInterface]):
        """
        Initialize the QQRunner with the specified batch system.

        Args:
            batch_system (Type[QQBatchInterface]): The batch system interface.

        Raises:
            QQError: If required environment variables are not set.
        """
        self.batch_system = batch_system

        self.username = os.environ.get(self.batch_system.usernameEnvVar())
        self.jobid = os.environ.get(self.batch_system.jobIdEnvVar())

        if not self.username or not self.jobid:
            raise QQError(
                f"Required {self.batch_system.envName()} environment variables are not set."
            )

    def setUpWorkDir(self, script: str):
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

        self._copyScriptToDir(Path(script), work_dir)
        os.chdir(work_dir)

        job_dir = os.environ.get(JOBDIR)
        if job_dir is None:
            raise QQError(f"'{JOBDIR}' environment variable is not set.")
        
        self._copyFilesToDst(self._getFilesToCopy(job_dir), work_dir)

        os.environ[WORKDIR] = str(work_dir)

    def executeScript(self, script: str) -> int:
        """
        Execute a script in the working directory, skipping its shebang line.

        Args:
            script (str): Path to the script file to execute.

        Returns:
            int: The return code from the script execution.

        Raises:
            QQError: If the script does not exist or execution fails.
        """
        script_path = Path(script)
        if not script_path.is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

        try:
            with script_path.open() as file:
                lines = file.readlines()[1:]

            result = subprocess.run(
                ["bash"], input = "".join(lines), text = True, check = False
            )

            return result.returncode

        except Exception as e:
            raise QQError(f"Failed to execute script '{script_path}': {e}")

    def _getScratchDir(self) -> Union[Path, None]:
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
                raise QQError(f"Could not set write permissions for '{directory}': {e}")

    def _createWorkDir(self, directory: Path):
        """
        Create a working directory for the job.

        Args:
            directory (Path): The path to the directory to create.

        Raises:
            QQError: If the directory cannot be created.
        """
        try:
            directory.mkdir(parents = False, exist_ok = False)
        except Exception as e:
            raise QQError(f"Could not create working directory '{directory}': {e}")

    def _copyScriptToDir(self, script: Path, directory: Path):
        """
        Copy a script file into a target directory.

        Args:
            script (Path): The script file to copy, relative to the batch system's (not qq's) working directory.
            directory (Path): The destination directory where the script will be copied.

        Raises:
            QQError: If the script cannot be copied to the destination directory.
        """
        try:
            shutil.copy(Path(self.batch_system.workDirEnvVar()) / script, directory)
        except Exception as e:
            raise QQError(f"Could not copy '{script}' into directory '{directory}': {e}")


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
                shutil.copy(file, directory)
            except Exception as e:
                raise QQError(f"Could not copy '{file}' into directory '{directory}': {e}")
    
    def _getFilesToCopy(self, directory: Path, filter_out: Optional[List[str]] = None) -> List[Path]:
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

        return [p for p in root.iterdir() if p.resolve() not in filter_paths]

        