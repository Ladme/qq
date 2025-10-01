# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
import socket
from pathlib import Path

from qq_lib.batch import QQBatchInterface
from qq_lib.common import is_printf_pattern, printf_to_regex
from qq_lib.constants import (
    ARCHIVER_RETRY_TRIES,
    ARCHIVER_RETRY_WAIT,
    LOOP_JOB_PATTERN,
    QQ_SUFFIXES,
)
from qq_lib.logger import get_logger
from qq_lib.retry import QQRetryer

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

        Existing directories are ignored without raising an error.
        """
        self._batch_system.makeRemoteDir(self._input_machine, self._archive)

    def archiveFrom(self, work_dir: Path, loop: int | None = None):
        """
        Fetch files from the archive to a local working directory.

        This method retrieves files from the archive that match the
        configured archive pattern. If a loop number is provided, only
        files corresponding to that loop (for printf-style patterns) are
        fetched. If no loop is provided, all files matching the pattern
        in the archive are fetched.

        Args:
            work_dir (Path): The local directory where files will be copied.
            loop (int | None): The loop number to filter files for.
                Only relevant for printf-style patterns. If `None`, all
                matching files are fetched. Defaults to `None`.

        Raises:
            QQError: If file transfer fails.
        """
        files = self._getFiles(
            self._archive, self._input_machine, self._archive_format, loop, False
        )

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
        files = self._getFiles(work_dir, None, self._archive_format, None, False)

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

    def archiveRunTimeFiles(self, job_name: str, loop: int):
        """
        Archive qq runtime files for a specific loop of the job located in the submission directory.

        The archived files are moved from the submission directory to the archive directory.
        Only qq runtime files corresponding to the given job name and loop number are archived.

        Args:
            job_name (str): The name of the job.
            loop (int): The loop number associated with the runtime files.

        Raises:
            QQError: If moving the runtime files fails after retries.

        """
        pattern = f"{job_name}{LOOP_JOB_PATTERN}"
        files = self._getFiles(
            self._job_dir, self._input_machine, pattern, loop, include_qq_files=True
        )
        moved_files = [self._archive / f.name for f in files]

        logger.debug(f"qq runtime files to archive: {files}.")
        logger.debug(f"qq runtime files after moving: {moved_files}.")

        QQRetryer(
            self._batch_system.moveRemoteFiles,
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
        loop: int | None = None,
        include_qq_files: bool = False,
    ) -> list[Path]:
        """
        Determine which files in a directory match a given pattern.

        Args:
            directory (Path): Directory to search for files.
            host (str | None): Hostname for remote directories, or None for local.
            pattern (str): A printf-style or regex pattern to match file stems.
            loop (int | None): Optional loop number for printf-style patterns.
                If provided, only files corresponding to that loop are returned.
                If `None`, all matching files are returned. Defaults to `None`.
            include_qq_files (bool): Whether to include qq runtime files. Defaults to False.

        Returns:
            list[Path]: A list of absolute paths to matching files.
        """
        if loop and is_printf_pattern(pattern):
            try:
                # try inserting the loop number into the printf pattern
                regex = re.compile(pattern % loop)
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
        if not host or host != socket.gethostname():
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
