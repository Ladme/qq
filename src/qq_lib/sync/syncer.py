# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from pathlib import Path

from rich.console import Console

from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.info.informer import QQInformer
from qq_lib.info.presenter import QQPresenter
from qq_lib.properties.states import RealState

logger = get_logger(__name__)


class QQSyncer:
    def __init__(self, info_file: Path):
        self._info_file = info_file
        self._informer = QQInformer.fromFile(self._info_file)
        self._batch_system = self._informer.batch_system
        self._state = self._informer.getRealState()

    def isFinished(self) -> bool:
        return self._state == RealState.FINISHED

    def isExitingSuccessfully(self) -> bool:
        return (
            self._state == RealState.EXITING and self._informer.info.job_exit_code == 0
        )

    def isQueued(self) -> bool:
        return self._state in {
            RealState.QUEUED,
            RealState.BOOTING,
            RealState.WAITING,
            RealState.HELD,
        }

    def sync(self, files: list[str] | None = None) -> None:
        """
        `files`: Names of files in the working directory.
        """
        self._setDestination()

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

    def printInfo(self, console: Console):
        """
        Display the current job status using a formatted panel.
        """
        presenter = QQPresenter(self._informer)
        panel = presenter.createJobStatusPanel(console)
        console.print(panel)

    def isJob(self, job_id: str) -> bool:
        """
        Determine whether this syncer corresponds to the specified job ID.

        Args:
            job_id (str): The job ID to compare against (e.g., "12345" or "12345.cluster.domain").

        Returns:
            bool: True if both job IDs refer to the same job (same numeric/job part),
                False otherwise.
        """
        return self._informer.isJob(job_id)

    def _setDestination(self):
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
            raise QQError(
                "Host ('main_node') or working directory ('work_dir') are not defined."
            )
