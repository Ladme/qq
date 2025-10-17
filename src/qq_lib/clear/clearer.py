# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from collections.abc import Iterable
from pathlib import Path

from qq_lib.core.common import get_info_files, get_runtime_files
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import QQInformer
from qq_lib.properties.states import RealState

logger = get_logger(__name__)


class QQClearer:
    """
    Handles detection and removal of qq runtime files from a directory.
    """

    def __init__(self, directory: Path):
        """
        Initialize a QQClearer for a specific directory.

        Args:
            directory (Path): The directory to clear qq runtime files from.
        """
        self._directory = directory

    def clear(self, force: bool = False) -> None:
        """
        Remove all qq runtime files from the directory that are safe to be removed.

        Only qq files that do **not** correspond to an active or successfully
        finished job will be removed, unless `force` is set to True.

        Args:
            force (bool): If True, remove all qq runtime files, even if unsafe.
        """
        # get all qq runtime files
        files = self._collectRunTimeFiles()
        logger.debug(f"All qq runtime files: {files}.")
        if not files:
            logger.info("Nothing to clear.")
            return

        # get files that should not be deleted
        excluded: set[Path] = self._collectExcludedFiles() if not force else set()
        logger.debug(f"Files excluded from clearing: {excluded}.")

        # get files that are safe to be deleted
        to_delete = files - excluded
        logger.debug(f"Files to delete: {to_delete}.")
        if not to_delete:
            logger.info(
                f"No qq files could be safely cleared. Rerun as '{CFG.binary_name} clear --force' to clear them forcibly."
            )
            return

        # remove the files that are safe to be deleted
        QQClearer._deleteFiles(to_delete)
        logger.info(
            f"Removed {len(to_delete)} qq file{'s' if len(to_delete) > 1 else ''}."
        )
        if excluded:
            logger.info(
                f"{len(excluded)} qq files were not safe to clear. Rerun as '{CFG.binary_name} clear --force' to clear them forcibly."
            )

    def _collectRunTimeFiles(self) -> set[Path]:
        """
        Collect all qq runtime files in the directory.

        Returns:
            set[Path]: Paths to all files matching qq-specific suffixes.
        """
        return set(get_runtime_files(self._directory))

    def _collectExcludedFiles(self) -> set[Path]:
        """
        Collect qq runtime files that should not be deleted.

        Runtime files corresponding to active or successfully finished jobs are included.

        Returns:
            set[Path]: Paths to qq runtime files that should not be deleted.
        """
        excluded = []

        # iterate through info files
        for file in get_info_files(self._directory):
            try:
                informer = QQInformer.fromFile(file)
                state = informer.getRealState()
                logger.debug(f"Job state: {str(state)}.")
            except QQError:
                # ignore the file if it cannot be read
                continue

            if state not in [
                RealState.KILLED,
                RealState.FAILED,
                RealState.IN_AN_INCONSISTENT_STATE,
            ]:
                excluded.append(file)  # qq info file
                excluded.append(
                    self._directory / informer.info.stdout_file
                )  # script stdout
                excluded.append(
                    self._directory / informer.info.stderr_file
                )  # script stderr
                excluded.append(
                    (self._directory / informer.info.job_name).with_suffix(
                        CFG.suffixes.qq_out
                    )
                )  # qq out file

        return set(excluded)

    @staticmethod
    def _deleteFiles(files: Iterable[Path]) -> None:
        """
        Delete all specified files.

        Args:
            files (Iterable[Path]): The list of files to delete.
        """
        for file in files:
            logger.debug(f"Removing file '{file}'.")
            file.unlink()
