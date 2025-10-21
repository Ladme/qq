# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from abc import ABC, abstractmethod
from datetime import timedelta


class BatchQueueInterface(ABC):
    """
    Abstract base class for retrieving and maintaining queue information
    from a batch scheduling system.

    The implementation of the constructor is arbitrary and should only
    be used inside the corresponding implementation of `QQBatchInterface.getQueues`.
    """

    @abstractmethod
    def update(self) -> None:
        """
        Refresh the stored queue information from the batch system.

        Raises:
            QQError: If the queue cannot be queried or its info updated.
        """
        pass

    @abstractmethod
    def getName(self) -> str:
        """
        Retrieve the name of the queue.

        Returns:
            str: The name identifying this queue in the batch system.
        """
        pass

    @abstractmethod
    def getPriority(self) -> int | None:
        """
        Retrieve the scheduling priority of the queue.

        Returns:
            int | None: The queue priority, or None if priority information
            is not available.
        """
        pass

    @abstractmethod
    def getTotalJobs(self) -> int:
        """
        Retrieve the total number of jobs currently in the queue.

        Returns:
            int: The total count of jobs, regardless of status.
        """
        pass

    @abstractmethod
    def getRunningJobs(self) -> int:
        """
        Retrieve the number of jobs currently running in the queue.

        Returns:
            int: The number of running jobs.
        """
        pass

    @abstractmethod
    def getQueuedJobs(self) -> int:
        """
        Retrieve the number of jobs waiting to start in the queue.

        Returns:
            int: The number of queued jobs.
        """
        pass

    @abstractmethod
    def getOtherJobs(self) -> int:
        """
        Retrieve the number of jobs in other states (non-running and non-queued).

        Returns:
            int: The number of jobs that are neither running nor queued,
            such as held or exiting jobs.
        """
        pass

    @abstractmethod
    def getMaxWalltime(self) -> timedelta | None:
        """
        Retrieve the maximum walltime allowed for jobs in the queue.

        Returns:
            timedelta | None: The walltime limit, or None if unlimited or unknown.
        """
        pass

    @abstractmethod
    def getComment(self) -> str:
        """
        Retrieve the comment or description associated with the queue.

        Returns:
            str: The human-readable comment or note about the queue.
        """
        pass

    @abstractmethod
    def isAvailableToUser(self, user: str) -> bool:
        """
        Check whether the specified user has access to this queue.

        Args:
            user (str): The username to check access for.

        Returns:
            bool: True if the user can submit jobs to this queue, False otherwise.
        """
        pass

    @abstractmethod
    def getDestinations(self) -> list[str]:
        """
        Retrieve all destinations available for this queue route.

        Returns:
            list[str]: A list of destination queue names associated with the queue.
        """
        pass

    @abstractmethod
    def fromRouteOnly(self) -> bool:
        """
        Determine whether this queue can only be accessed via a route.

        Returns:
            bool: True if the queue is accessible exclusively through a route,
            False otherwise.
        """
        pass

    @abstractmethod
    def toYaml(self) -> str:
        """
        Return all information about the queue from the batch system in YAML format.

        Returns:
            str: YAML-formatted string of queue metadata.
        """
        pass
