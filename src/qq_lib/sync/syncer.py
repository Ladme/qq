# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from pathlib import Path

from rich.console import Console

from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import QQInformer
from qq_lib.info.presenter import QQPresenter
from qq_lib.properties.states import RealState

logger = get_logger(__name__)


class QQSyncer:
    """
    Handle synchronization of job files between a remote working directory
    (on a compute node or cluster) and the local input directory.
    """

    def __init__(self, info_file: Path):
        """
        Initialize the synchronizer for a given job.

        Args:
            info_file (Path): Path to the job's qq info file.
        """
        self._info_file = info_file
        self._informer = QQInformer.fromFile(self._info_file)
        self._batch_system = self._informer.batch_system
        self._state = self._informer.getRealState()

        self._setDestination()

    def printInfo(self, console: Console) -> None:
        """
        Display the current job information in a formatted Rich panel.

        Args:
            console (Console): Rich Console instance used to render output.
        """
        presenter = QQPresenter(self._informer)
        panel = presenter.createJobStatusPanel(console)
        console.print(panel)

    def hasDestination(self) -> bool:
        """
        Check that the job has an assigned host and working directory.

        Returns:
            bool: True if the job has both a host and a working directory,
            False otherwise.
        """
        return self._directory is not None and self._host is not None

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

    def matchesJob(self, job_id: str) -> bool:
        """
        Determine whether this syncer corresponds to the specified job ID.

        Args:
            job_id (str): The job ID to compare against (e.g., "12345" or "12345.cluster.domain").

        Returns:
            bool: True if both job IDs refer to the same job (same numeric/job part),
                False otherwise.
        """
        return self._informer.matchesJob(job_id)

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
                self._directory,
                self._informer.info.input_dir,
                self._host,
                None,
                [self._directory / x for x in files],
            )
        else:
            logger.info(
                "Fetching all files from job's working directory to input directory."
            )
            self._batch_system.syncWithExclusions(
                self._directory, self._informer.info.input_dir, self._host, None
            )

    def _setDestination(self) -> None:
        """
        Get the job's host and working directory from the QQInformer.

        Updates:
            - _host: hostname of the node where the job runs
            - _directory: absolute path to the working directory

        Raises:
            QQError: If main_node or work_dir are not defined.
        """
        destination = self._informer.getDestination()
        logger.debug(f"Destination: {destination}.")

        if destination:
            (self._host, self._directory) = destination
        else:
            self._host = None
            self._directory = None

    def _isKilled(self) -> bool:
        """Check if the job has been or is being killed."""
        return self._state == RealState.KILLED or (
            self._state == RealState.EXITING
            and self._informer.info.job_exit_code is None
        )

    def _isFinished(self) -> bool:
        """Check if the job has finished succesfully."""
        return self._state == RealState.FINISHED

    def _isExitingSuccessfully(self) -> bool:
        """
        Check whether the job is currently successfully exiting.
        """
        return (
            self._state == RealState.EXITING and self._informer.info.job_exit_code == 0
        )

    def _isQueued(self) -> bool:
        """
        Check if the job is still waiting to start or booting.
        """
        return self._state in {
            RealState.QUEUED,
            RealState.BOOTING,
            RealState.WAITING,
            RealState.HELD,
        }
