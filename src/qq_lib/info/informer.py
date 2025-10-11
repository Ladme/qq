# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime
from pathlib import Path
from typing import Self

from qq_lib.batch.interface import BatchJobInfoInterface, QQBatchInterface
from qq_lib.core.logger import get_logger
from qq_lib.properties.info import QQInfo
from qq_lib.properties.states import BatchState, NaiveState, RealState

logger = get_logger(__name__)


class QQInformer:
    """
    Provides an interface to access and manipulate qq job information.
    """

    def __init__(self, info: QQInfo):
        """
        Initialize the informer with job information.

        Args:
            info: A QQInfo object containing raw job data.
        """
        self.info = info
        self._batch_info: BatchJobInfoInterface | None = None

    @property
    def batch_system(self) -> type[QQBatchInterface]:
        """
        Return the batch system class used for this job.

        Returns:
            The QQBatchInterface implementation associated with the job.
        """
        return self.info.batch_system

    @classmethod
    def fromFile(cls, file: Path, host: str | None = None) -> Self:
        """
        Create a QQInformer by loading job information from a file.

        If 'host' is provided, the file is read from the remote host; otherwise, it is read locally.

        Args:
            file (Path): Path to a YAML file containing job information.
            host (str | None): Optional remote host from which to read the file.

        Returns:
            QQInformer: An instance initialized with the loaded QQInfo.

        Raises:
            QQError: If the file cannot be read, reached, or parsed correctly.
        """
        return cls(QQInfo.fromFile(file, host))

    def toFile(self, file: Path, host: str | None = None):
        """
        Export the job information to a file.

        If `host` is provided, the file is written to the remote host; otherwise, it is written locally.

        Args:
            file (Path): Path to the output YAML file.
            host (str | None): Optional remote host where the file should be written.

        Raises:
            QQError: If the file cannot be created, reached, or written to.
        """
        self.info.toFile(file, host)

    def setRunning(
        self, time: datetime, main_node: str, all_nodes: list[str], work_dir: Path
    ):
        """
        Mark the job as running and set associated metadata.

        Args:
            time: Job start time.
            main_node: Main node assigned to the job.
            work_dir: Working directory used by the job.
        """
        self.info.job_state = NaiveState.RUNNING
        self.info.start_time = time
        self.info.main_node = main_node
        self.info.all_nodes = all_nodes
        self.info.work_dir = work_dir

    def setFinished(self, time: datetime):
        """
        Mark the job as finished successfully.

        Args:
            time: Job completion time.
        """
        self.info.job_state = NaiveState.FINISHED
        self.info.completion_time = time
        self.info.job_exit_code = 0

    def setFailed(self, time: datetime, exit_code: int):
        """
        Mark the job as failed.

        Args:
            time: Job completion (failure) time.
            exit_code: Exit code of the failed job.
        """
        self.info.job_state = NaiveState.FAILED
        self.info.completion_time = time
        self.info.job_exit_code = exit_code

    def setKilled(self, time: datetime):
        """
        Mark the job as killed.

        Args:
            time: Time when the job was killed.
        """
        self.info.job_state = NaiveState.KILLED
        self.info.completion_time = time
        # no exit code is intentionally set

    def useScratch(self) -> bool:
        """
        Determine if the job uses a scratch directory.

        Returns:
            nool: True if a scratch is used, False if it is not.
        """
        return self.info.resources.useScratch()

    def getDestination(self) -> tuple[str, Path] | None:
        """
        Retrieve the job's main node and working directory.

        Returns:
            tuple[str, Path] | None: A tuple of (main_node, work_dir)
                if both are set, otherwise None.
        """
        if all((self.info.main_node, self.info.work_dir)):
            return self.info.main_node, self.info.work_dir
        return None

    def getBatchState(self) -> BatchState:
        """
        Return the job's state as reported by the batch system.

        Uses cached information if available; otherwise queries the batch system
        via `batch_system.getJobInfo`. This avoids unnecessary remote calls.

        Returns:
            BatchState: The job's state according to the batch system.
        """
        if not self._batch_info:
            self._batch_info = self.batch_system.getJobInfo(self.info.job_id)

        return self._batch_info.getState()

    def getRealState(self) -> RealState:
        """
        Get the job's real state by combining qq's internal state (`NaiveState`)
        with the state reported by the batch system (`BatchState`).

        Uses cached information if available; otherwise queries the batch system
        via `batch_system.getJobInfo`. This avoids unnecessary remote calls.

        Returns:
            RealState: The job's real state obtained by combining information
            from qq and the batch system.
        """
        # shortcut: if the naive state is unknown, there is no need to check batch state
        if self.info.job_state in {
            NaiveState.UNKNOWN,
        }:
            logger.debug(
                "Short-circuiting getRealState: the batch state will not affect the result."
            )
            return RealState.fromStates(self.info.job_state, BatchState.UNKNOWN)

        return RealState.fromStates(self.info.job_state, self.getBatchState())

    def getComment(self) -> str | None:
        """
        Return the job's comment as reported by the batch system.

        Uses cached information if available; otherwise queries the batch system
        via `batch_system.getJobInfo`. This avoids unnecessary remote calls.

        Returns:
            str | None: The job comment if available, otherwise None.
        """
        if not self._batch_info:
            self._batch_info = self.batch_system.getJobInfo(self.info.job_id)

        return self._batch_info.getComment()

    def getEstimated(self) -> tuple[datetime, str] | None:
        """
        Return the estimated start time and execution node for the job.

        Uses cached information if available; otherwise queries the batch system
        via `batch_system.getJobInfo`. This avoids unnecessary remote calls.

        Returns:
            tuple[datetime, str] | None: A tuple containing the estimated start time
            (as a datetime) and the execution node (as a string), or None if the
            information is not available.
        """
        if not self._batch_info:
            self._batch_info = self.batch_system.getJobInfo(self.info.job_id)

        return self._batch_info.getEstimated()

    def getMainNode(self) -> str | None:
        """
        Return the main execution node for the job.

        Note that this obtains the node information from the batch system itself!

        Uses cached information if available; otherwise queries the batch system
        via `batch_system.getJobInfo`. This avoids unnecessary remote calls.

        Returns:
            str | None: The hostname of the main execution node, or None if the
            information is not available.
        """
        if not self._batch_info:
            self._batch_info = self.batch_system.getJobInfo(self.info.job_id)

        return self._batch_info.getMainNode()

    def getNodes(self) -> list[str] | None:
        """
        Retrieve the list of execution nodes on which the job is running.

        Note that this obtains the node information from the batch system itself!

        Uses cached information if available; otherwise queries the batch system
        via `batch_system.getJobInfo`. This avoids unnecessary remote calls.

        Returns:
            list[str] | None:
                A list of hostnames (or node identifiers) where the job is running,
                or `None` if the job has not started or node information is unavailable.
        """
        if not self._batch_info:
            self._batch_info = self.batch_system.getJobInfo(self.info.job_id)

        return self._batch_info.getNodes()

    def isJob(self, job_id: str) -> bool:
        """
        Determine whether this informer corresponds to the specified job ID.

        Args:
            job_id (str): The job ID to compare against (e.g., "12345" or "12345.cluster.domain").

        Returns:
            bool: True if both job IDs refer to the same job (same numeric/job part),
                False otherwise.
        """
        return self.info.job_id.split(".", 1)[0] == job_id.split(".", 1)[0]
