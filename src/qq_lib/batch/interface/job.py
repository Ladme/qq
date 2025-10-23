# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path

from qq_lib.properties.size import Size
from qq_lib.properties.states import BatchState


class BatchJobInterface(ABC):
    """
    Abstract base class for retrieving and maintaining job information
    from a batch scheduling system.

    Must support situations where the job information no longer exists.

    The implementation of the constructor is arbitrary and should only
    be used inside the corresponding implementation of `QQBatchInterface.getBatchJob`.
    """

    @abstractmethod
    def isEmpty(self) -> bool:
        """
        Check whether the job contains any information.
        This should return True if the job does not exist in the batch system.

        Returns:
            bool: True if the job contains no information.
        """
        pass

    @abstractmethod
    def getId(self) -> str:
        """
        Return the ID of the job.

        Returns:
            str: The ID of the job.
        """
        pass

    @abstractmethod
    def update(self) -> None:
        """
        Refresh the stored job information from the batch system.

        Raises:
            QQError: If the job cannot be queried or its info updated.
        """
        pass

    @abstractmethod
    def getState(self) -> BatchState:
        """
        Return the current state of the job as reported by the batch system.

        If the job information is no longer available, return `BatchState.UNKNOWN`.

        Returns:
            BatchState: The job state according to the batch system.
        """
        pass

    @abstractmethod
    def getComment(self) -> str | None:
        """
        Retrieve the batch system-provided comment for the job.

        Returns:
            str | None: The job's comment string if available, or None if the
            batch system has not attached a comment.
        """
        pass

    @abstractmethod
    def getEstimated(self) -> tuple[datetime, str] | None:
        """
        Retrieve the batch system's estimated job start time and execution node.

        Returns:
            tuple[datetime, str] | None: A tuple containing:
                - datetime: The estimated start time of the job.
                - str: The name of the node where the job is expected to run.
            Returns None if either estimate is unavailable.
        """
        pass

    @abstractmethod
    def getMainNode(self) -> str | None:
        """
        Retrieve the hostname of the main execution node for the job.

        Returns:
            str | None: The hostname of the main execution node, or ``None``
            if unavailable or not applicable.
        """
        pass

    @abstractmethod
    def getNodes(self) -> list[str] | None:
        """
        Retrieve the hostnames of all execution nodes allocated for the job.

        Returns:
            list[str] | None:
                A list of hostnames or node identifiers used by the job,
                or `None` if node information is not available.
        """
        pass

    @abstractmethod
    def getShortNodes(self) -> list[str] | None:
        """
        Retrieve the short hostnames of all execution nodes allocated for the job.

        Returns:
            list[str] | None:
                A list of short hostnames used by the job, or `None` if node information
                is not available.
        """
        pass

    @abstractmethod
    def getUser(self) -> str:
        """
        Return the username of the job owner.

        Returns:
            str: Username of the user who owns the job.
        """
        pass

    @abstractmethod
    def getNCPUs(self) -> int:
        """
        Return the number of CPU cores allocated for the job.

        Returns:
            int: Number of CPUs allocated for the job.
        """
        pass

    @abstractmethod
    def getNGPUs(self) -> int:
        """
        Return the number of GPUs allocated for the job.

        Returns:
            int: Number of GPUs allocated for the job.
        """
        pass

    @abstractmethod
    def getNNodes(self) -> int:
        """
        Return the number of compute nodes assigned to the job.

        Returns:
            int: Number of nodes used by the job.
        """
        pass

    @abstractmethod
    def getMem(self) -> Size:
        """
        Return the amount of memory allocated for the job.

        Returns:
            Size: Amount of memory allocated for the job.
        """
        pass

    @abstractmethod
    def getName(self) -> str:
        """
        Return the name of the job.

        Returns:
            str: The name of the submitted job.
        """
        pass

    @abstractmethod
    def getSubmissionTime(self) -> datetime:
        """
        Return the timestamp when the job was submitted.

        Returns:
            datetime: Time when the job was submitted to the batch system.
        """
        pass

    @abstractmethod
    def getStartTime(self) -> datetime | None:
        """
        Return the timestamp when the job started execution.

        Returns:
            datetime | None: Time when the job began running or
            None if the job has not yet started.
        """
        pass

    @abstractmethod
    def getCompletionTime(self) -> datetime | None:
        """
        Return the timestamp when the job was completed.

        Returns:
            datetime | None: Time when the job completed or
            None if the job has not yet completed.
        """
        pass

    @abstractmethod
    def getModificationTime(self) -> datetime:
        """
        Return the timestamp at which the job was last modified.

        Returns:
            datetime: Time when the job was last modified.
        """
        pass

    @abstractmethod
    def getWalltime(self) -> timedelta:
        """
        Return the walltime limit of the job.

        Returns:
            timedelta: Walltime for the job.
        """
        pass

    @abstractmethod
    def getQueue(self) -> str:
        """
        Return the submission queue of the job.

        Returns:
            str: The queue this job is part of.
        """
        pass

    @abstractmethod
    def getUtilCPU(self) -> int | None:
        """
        Return the utilization of requested CPUs in percents (0-100).

        Returns:
            int | None: Utilization of requested CPUs or None
            if information is not available.
        """
        pass

    @abstractmethod
    def getUtilMem(self) -> int | None:
        """
        Return the utilization of requested memory in percents (0-100).

        Returns:
            int | None: Utilization of requested memory or None
            if information is not available.
        """
        pass

    @abstractmethod
    def getExitCode(self) -> int | None:
        """
        Return the exit code of the job.

        Returns:
            int | None: Exit code of the job or None
            if exit code is not assigned.
        """
        pass

    @abstractmethod
    def getInputDir(self) -> Path:
        """
        Return path to the directory from which the job was submitted.

        Returns:
            Path: Path to the submission directory.
        """
        pass

    @abstractmethod
    def getInputMachine(self) -> str:
        """
        Return the hostname of the submission machine.

        Returns:
            str: Hostname of the submission machine.
        """
        pass

    @abstractmethod
    def getInfoFile(self) -> Path | None:
        """
        Return path to the info file associated with this job.

        Returns:
            Path | None: Path to the qq info file or `None` if
            this is not a qq job.
        """
        pass

    @abstractmethod
    def toYaml(self) -> str:
        """
        Return all information about the job from the batch system in YAML format.

        Returns:
            str: YAML-formatted string of job metadata.
        """
        pass
