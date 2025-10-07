# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
import socket
from pathlib import Path

from qq_lib.batch.interface import QQBatchInterface
from qq_lib.core.common import is_printf_pattern, printf_to_regex
from qq_lib.core.constants import (
    ARCHIVER_RETRY_TRIES,
    ARCHIVER_RETRY_WAIT,
    QQ_SUFFIXES,
)
from qq_lib.core.logger import get_logger
from qq_lib.core.retryer import QQRetryer

logger = get_logger(__name__, show_time=True)


class QQArchiver:
    """
    Handles archiving and retrieval of files for a job.
    """

    def __init__(
        self,
        archive: Path,
        archive_format: str,
        input_machine: str,
        job_dir: Path,
        batch_system: type[QQBatchInterface],
    ):
        """
        Initialize the QQArchiver.

        Args:
            archive (Path): Absolute path to the archive directory.
            archive_format (str): The pattern describing which files to archive.
            input_machine (str): The hostname from which the job was submitted.
            job_dir (Path): The directory from which the job was submitted.
            batch_system (type[QQBatchInterface]): The batch system used to run the qq job.
        """
        self._batch_system = batch_system
        self._archive = archive
        self._archive_format = archive_format
        self._input_machine = input_machine
        self._job_dir = job_dir

    def makeArchiveDir(self):
        """
        Create the archive directory if it does not already exist.
        """
        logger.debug(
            f"Attempting to create an archive '{self._archive}' on '{self._input_machine}'."
        )
        self._batch_system.makeRemoteDir(self._input_machine, self._archive)

    def archiveFrom(self, work_dir: Path, cycle: int | None = None):
        """
        Fetch files from the archive to a local working directory.

        This method retrieves files from the archive that match the
        configured archive pattern. If a cycle number is provided, only
        files corresponding to that cycle (for printf-style patterns) are
        fetched. If no cycle is provided, all files matching the pattern
        in the archive are fetched.

        Args:
            work_dir (Path): The local directory where files will be copied.
            cycle (int | None): The cycle number to filter files for.
                Only relevant for printf-style patterns. If `None`, all
                matching files are fetched. Defaults to `None`.

        Raises:
            QQError: If file transfer fails.
        """
        if not (
            files := self._getFiles(
                self._archive, self._input_machine, self._archive_format, cycle, False
            )
        ):
            logger.debug("Nothing to fetch from archive.")
            return

        logger.debug(f"Files to fetch from archive: {files}.")

        QQRetryer(
            self._batch_system.syncSelected,
            self._archive,
            work_dir,
            self._input_machine,
            socket.gethostname(),
            files,
            max_tries=ARCHIVER_RETRY_TRIES,
            wait_seconds=ARCHIVER_RETRY_WAIT,
        ).run()

    def archiveTo(self, work_dir: Path):
        """
        Archive all files matching the archive format in the specified working directory.

        Copies all files matching the archive pattern from the local
        `work_dir` to the archive directory. After successfully transferring
        the files, they are removed from the working directory.

        Args:
            work_dir (Path): The local directory containing files to archive.

        Raises:
            QQError: If file transfer or removal fails.
        """
        if not (
            files := self._getFiles(work_dir, None, self._archive_format, None, False)
        ):
            logger.debug("Nothing to archive.")
            return

        logger.debug(f"Files to archive: {files}.")

        QQRetryer(
            self._batch_system.syncSelected,
            work_dir,
            self._archive,
            socket.gethostname(),
            self._input_machine,
            files,
            max_tries=ARCHIVER_RETRY_TRIES,
            wait_seconds=ARCHIVER_RETRY_WAIT,
        ).run()

        # remove the archived files
        QQRetryer(
            self._removeFiles,
            files,
            max_tries=ARCHIVER_RETRY_TRIES,
            wait_seconds=ARCHIVER_RETRY_WAIT,
        ).run()

    def archiveRunTimeFiles(self, job_name: str, cycle: int):
        """
        Archive qq runtime files from a specific job located in the submission directory.

        The archived files are moved from the submission directory to the archive directory.

        Ensure that `job_name` does not contain special regex characters, or that any such
        characters are properly escaped.

        This function will archive all files whose names match `job_name`, regardless
        of whether they have any qq-specific suffixes.

        Args:
            job_name (str): The name of the job.
            cycle (int): Current cycle number (for archiving).

        Raises:
            QQError: If moving the runtime files fails.
        """
        if not (
            files := self._getFiles(
                self._job_dir,
                self._input_machine,
                job_name,
                cycle=None,
                include_qq_files=True,
            )
        ):
            logger.debug("No qq runtime files to archive.")
            return

        # the files are renamed to conform the the archive format
        moved_files = [
            self._archive / f"{self._archive_format % cycle}{f.suffix}" for f in files
        ]

        logger.debug(f"qq runtime files to archive: {files}.")
        logger.debug(f"qq runtime files after moving: {moved_files}.")

        QQRetryer(
            self._batch_system.moveRemoteFiles,
            self._input_machine,
            files,
            moved_files,
            max_tries=ARCHIVER_RETRY_TRIES,
            wait_seconds=ARCHIVER_RETRY_WAIT,
        ).run()

    def _getFiles(
        self,
        directory: Path,
        host: str | None,
        pattern: str,
        cycle: int | None = None,
        include_qq_files: bool = False,
    ) -> list[Path]:
        """
        Determine which files in a directory match a given pattern.

        Args:
            directory (Path): Directory to search for files.
            host (str | None): Hostname for remote directories, or None for local.
            pattern (str): A printf-style or regex pattern to match file stems.
            cycle (int | None): Optional cycle number for printf-style patterns.
                If provided, only files corresponding to that loop are returned.
                If `None`, all matching files are returned. Defaults to `None`.
            include_qq_files (bool): Whether to include qq runtime files. Defaults to False.

        Returns:
            list[Path]: A list of absolute paths to matching files.
        """
        if cycle and is_printf_pattern(pattern):
            try:
                # try inserting the loop number into the printf pattern
                regex = re.compile(f"^{pattern % cycle}$")
            except Exception:
                logger.debug(
                    f"Ignoring loop number since the provided pattern ('{pattern}') does not support it."
                )
                regex = QQArchiver._prepare_regex_pattern(pattern)
        else:
            logger.debug(
                f"Loop number not specified or the provided pattern ('{pattern}') does not support it."
            )
            regex = QQArchiver._prepare_regex_pattern(pattern)

        logger.debug(f"Regex for matching: {regex}.")

        # the directory must exist
        if host and host != socket.gethostname():
            # remote directory
            available_files: list[Path] = QQRetryer(
                self._batch_system.listRemoteDir,
                host,
                directory,
                max_tries=ARCHIVER_RETRY_TRIES,
                wait_seconds=ARCHIVER_RETRY_WAIT,
            ).run()
        else:
            # local directory
            available_files = list(directory.iterdir())

        logger.debug(f"All available files: {available_files}.")
        if include_qq_files:
            return [f.resolve() for f in available_files if regex.fullmatch(f.stem)]
        return [
            f.resolve()
            for f in available_files
            if regex.fullmatch(f.stem) and f.suffix not in QQ_SUFFIXES
        ]

    @staticmethod
    def _prepare_regex_pattern(pattern: str) -> re.Pattern[str]:
        """
        Convert a printf-style pattern or regex string into a compiled regex.

        Args:
            pattern (str): The pattern to convert.

        Returns:
            re.Pattern[str]: Compiled regex pattern matching the entire filename stem.
        """
        if is_printf_pattern(pattern):
            pattern = printf_to_regex(pattern)
        else:
            # make sure that regex matches the entire filename
            if not pattern.startswith("^"):
                pattern = f"^{pattern}"
            if not pattern.endswith("$"):
                pattern = f"{pattern}$"

        return re.compile(pattern)

    @staticmethod
    def _removeFiles(files: list[Path]):
        """
        Remove a list of files from the filesystem.

        Args:
            files (list[Path]): Files to delete.

        Raises:
            OSError: If file removal fails for any file.
        """
        for file in files:
            file.unlink()
