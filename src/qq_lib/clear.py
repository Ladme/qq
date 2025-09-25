# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
This module provides a tool for safely clearing qq run files from a job directory.

The process goes as follows:
    - the QQClearer scans the directory for files with qq-specific suffixes.
    - it checks whether it is safe to remove files
      (all jobs must be in a failed/killed/inconsistent state or `--force` must be used).
    - files are removed if clearing is allowed.
"""

import sys
from pathlib import Path

import click

from qq_lib.common import get_files_with_suffix, get_info_files
from qq_lib.constants import QQ_SUFFIXES
from qq_lib.error import QQError
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger
from qq_lib.states import RealState

logger = get_logger(__name__)


@click.command(help="Delete qq run files.")
@click.option(
    "--force",
    is_flag=True,
    help="Clear directory with an active or successful job.",
    default=False,
)
def clear(force: bool):
    """
    Delete all qq run files in the current directory.
    """
    try:
        clearer = QQClearer(Path())
        files = clearer.getQQFiles()
        clearer.clearFiles(files, force)
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


class QQClearer:
    """
    Handles detection and removal of qq run files from a directory.
    """

    def __init__(self, directory: Path):
        """
        Initialize a QQClearer for a specific directory.

        Args:
            directory (Path): The directory to clear qq run files from.
        """
        self._directory = directory

    def getQQFiles(self) -> list[Path]:
        """
        Get a list of all qq-related files in the directory.

        Returns:
            list[Path]: Paths to all files matching qq-specific suffixes.
        """
        files = []
        for suffix in QQ_SUFFIXES:
            files.extend(get_files_with_suffix(self._directory, suffix))

        return files

    def clearFiles(self, files: list[Path], force: bool):
        """
        Remove the specified qq files from the directory if it is safe to do so.

        Args:
            files (list[Path]): The files to remove.
            force (bool): Whether to forcibly remove files even if the job is active.

        Raises:
            QQError: If clearing may corrupt or delete useful data and --force is not used.
        """
        if len(files) == 0:
            return

        if self.shouldClear(force):
            for file in files:
                logger.debug(f"Removing file '{file}'.")
                Path.unlink(file)
            logger.info(
                f"Removed {len(files)} qq run file{'s' if len(files) != 1 else ''}."
            )
        else:
            raise QQError(
                "Clearing this qq job directory may corrupt or delete useful data. Use 'qq clear --force' if sure."
            )

    def shouldClear(self, force: bool) -> bool:
        """
        Determine whether it is safe to clear qq files from the directory.

        Args:
            force (bool): If True, allows clearing regardless of job state.

        Returns:
            bool: True if clearing is allowed, False otherwise.

        Notes:
            - Jobs in KILLED, FAILED, or INCONSISTENT states are safe to clear.
            - If no qq info file can be loaded, the directory is assumed safe to clear.
        """
        if force:
            return True

        # iterate through info files
        for file in get_info_files(self._directory):
            try:
                informer = QQInformer.fromFile(file)
                state = informer.getRealState()
                logger.debug(f"Job state: {str(state)}.")
            except QQError:
                # ignore the file if it cannot be read
                continue

            # if any info file is active or finished, return False
            if state not in {
                RealState.KILLED,
                RealState.FAILED,
                RealState.IN_AN_INCONSISTENT_STATE,
            }:
                return False

        # we return true if all info files are from invalid runs
        return True
