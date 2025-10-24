# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.core.navigator import QQNavigator

logger = get_logger(__name__)


class QQSyncer(QQNavigator):
    """
    Handle synchronization of job files between a remote working directory
    (on a compute node or cluster) and the local input directory.
    """

    def ensureSuitable(self):
        """
        Verify that the job is in a state where files
        can be fetched from its working directory.

        Raises:
            QQNotSuitableError: If the job has already finished / is finishing successfully
                                is queued/booting or has been killed without creating a working directory.
        """
        # finished jobs do not have working directory
        if self._isFinished():
            raise QQNotSuitableError(
                "Job has finished and was synchronized: nothing to sync."
            )

        # killed jobs may not have working directory
        if self._isKilled() and not self.hasDestination():
            raise QQNotSuitableError(
                "Job has been killed and no working directory is available."
            )

        # succesfully exiting jobs do not have working directory
        if self._isExitingSuccessfully():
            raise QQNotSuitableError("Job is finishing successfully: nothing to sync.")

        # queued jobs do not have working directory
        if self._isQueued():
            raise QQNotSuitableError("Job is queued or booting: nothing to sync.")

    def sync(self, files: list[str] | None = None) -> None:
        """
        Synchronize files from the remote working directory to the local input directory.

        Args:
            files (list[str] | None): Optional list of specific filenames to fetch.
                If omitted, all files are synchronized except those excluded by the batch system.

        Behavior:
            - If `files` is provided, only those specific files are copied.
            - If omitted, the entire working directory is synchronized.

        Raises:
            QQError: If the job's destination (host or working directory) cannot be determined.
        """
        if not self.hasDestination():
            raise QQError(
                "Host ('main_node') or working directory ('work_dir') are not defined."
            )

        if files:
            logger.info(
                f"Fetching file{'s' if len(files) > 1 else ''} '{' '.join(files)}' from job's working directory to input directory."
            )
            self._batch_system.syncSelected(
                self._work_dir,
                self._informer.info.input_dir,
                self._main_node,
                None,
                [self._work_dir / x for x in files],  # ty: ignore[unsupported-operator]
            )
        else:
            logger.info(
                "Fetching all files from job's working directory to input directory."
            )
            self._batch_system.syncWithExclusions(
                self._work_dir, self._informer.info.input_dir, self._main_node, None
            )
