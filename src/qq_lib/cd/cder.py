# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path

from qq_lib.batch.interface import BatchJobInterface, QQBatchInterface
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)


class QQCder:
    """
    Retrieve and provide the input directory for a specific job
    in the configured batch system.
    """

    def __init__(self, BatchSystem: type[QQBatchInterface], job_id: str):
        """
        Initialize the QQCder instance with a batch system interface and job ID.

        Args:
            BatchSystem (type[QQBatchInterface]): Batch system class to use.
            job_id (str): Identifier of the job whose input directory is needed.
        """
        self._job_id = job_id
        self._BatchSystem = BatchSystem

    def cd(self) -> str:
        """
        Retrieve the input directory for the job and return it as a string.

        Returns:
            str: Path to the input directory of the job.

        Raises:
            QQError: If the job does not exist.
        """
        path = QQCder._getInputDirFromJobId(self._BatchSystem, self._job_id)
        logger.debug(f"Changing directory to '{path}'.")
        return str(path)

    @staticmethod
    def _getInputDirFromJobId(BatchSystem: type[QQBatchInterface], job_id: str) -> Path:
        """
        Query the batch system for the input/submission directory of a job.

        Args:
            BatchSystem (type[QQBatchInterface]): Batch system class to use.
            job_id (str): Identifier of the job to query.

        Returns:
            Path: Path object pointing to the job's input directory.

        Raises:
            QQError: If the specified job does not exist.
        """
        job_info: BatchJobInterface = BatchSystem.getBatchJob(job_id)

        if job_info.isEmpty():
            raise QQError(f"Job '{job_id}' does not exist.")

        return job_info.getInputDir()
