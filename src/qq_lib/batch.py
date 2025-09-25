# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from abc import ABC, ABCMeta, abstractmethod
from pathlib import Path

from qq_lib.error import QQError
from qq_lib.resources import QQResources
from qq_lib.states import BatchState


class BatchJobInfoInterface(ABC):
    """
    Abstract base class for retrieving and maintaining job information
    from a batch scheduling system.

    Must support situations where the job information no longer exists.

    The implementation of the constructor is arbitrary and should only
    be used inside the corresponding implementation of ``QQBatchInterface.getJobInfo``.
    """

    @abstractmethod
    def update(self):
        """
        Refresh the stored job information from the batch system.

        Raises:
            QQError: If the job cannot be queried or updated.
        """
        pass

    @abstractmethod
    def getJobState(self) -> BatchState:
        """
        Return the current state of the job as reported by the batch system.

        If the job information is no longer available, return ``BatchState.UNKNOWN``.

        Returns:
            BatchState: The job state according to the batch system.
        """
        pass


class QQBatchInterface[TBatchInfo: BatchJobInfoInterface](ABC):
    """
    Abstract base class for batch system integrations.

    Concrete batch system classes must implement these methods to allow
    qq to interact with different batch systems uniformly.

    All functions should raise QQError when encountering an error.
    """

    @staticmethod
    @abstractmethod
    def envName() -> str:
        """
        Return the name of the batch system environment.

        Returns:
            str: The batch system name.
        """
        pass

    @staticmethod
    @abstractmethod
    def getScratchDir(job_id: str) -> Path:
        """
        Retrieve the scratch directory for a given job.

        Args:
            job_id (int): Unique identifier of the job.

        Returns:
            Path: Path to the scratch directory.

        Raises:
            QQError: If there is no scratch directory available for this job.
        """
        pass

    @staticmethod
    @abstractmethod
    def jobSubmit(res: QQResources, queue: str, script: Path) -> str:
        """
        Submit a job to the batch system.

        Args:
            res (QQResources): Resources required for the job.
            queue (str): Target queue for the job submission.
            script (Path): Path to the script to execute.

        Returns:
            str: Unique ID of the submitted job.

        Raises:
            QQError: If the job submission fails.
        """
        pass

    @staticmethod
    @abstractmethod
    def jobKill(job_id: str):
        """
        Terminate a job gracefully. This assumes that job has time for cleanup.

        Args:
            job_id (str): Identifier of the job to terminate.

        Raises:
            QQError: If the job could not be killed.
        """
        pass

    @staticmethod
    @abstractmethod
    def jobKillForce(job_id: str):
        """
        Forcefully terminate a job. This assumes that the job has no time for cleanup.

        Args:
            job_id (str): Identifier of the job to forcefully terminate.

        Raises:
            QQError: If the job could not be killed.
        """
        pass

    @staticmethod
    @abstractmethod
    def navigateToDestination(host: str, directory: Path):
        """
        Navigate to a directory on the specified host.

        Args:
            host (str): Target hostname where the directory resides.
            directory (Path): Path to navigate to.

        Raises:
            QQError: If the navigation fails.
        """
        pass

    @staticmethod
    @abstractmethod
    def getJobInfo(job_id: str) -> TBatchInfo:
        """
        Retrieve comprehensive information about a job.

        The returned object should be fully initialized, even if the job
        no longer exists or its information is unavailable.

        Args:
            job_id (str): Identifier of the job.

        Returns:
            TBatchInfo: Object containing the job's metadata and state.
        """
        pass

    @staticmethod
    def buildResources(**kwargs) -> QQResources:
        """
        Build a QQResources object for the target batch system using input parameters.

        By default, this method constructs basic resources directly from `kwargs`
        without performing any additional validation. The `kwargs` dictionary contains
        parameters provided to `qq submit`. Implementations can override this method
        to add validation or transform the input as needed.

        Raises:
            QQError: If any required parameters are missing or invalid.
        """
        del kwargs["batch_system"]
        return QQResources(**kwargs)


class QQBatchMeta(ABCMeta):
    """
    Metaclass for batch system classes.
    """

    # registry of supported batch systems
    _registry: dict[str, type[QQBatchInterface]] = {}

    def __str__(cls: type[QQBatchInterface]):
        """
        Get the string representation of the batch system class.
        """
        return cls.envName()

    @classmethod
    def register(cls, batch_cls: type[QQBatchInterface]):
        """
        Register a batch system class in the metaclass registry.

        Args:
            batch_cls: Subclass of QQBatchInterface to register.
        """
        cls._registry[batch_cls.envName()] = batch_cls

    @classmethod
    def fromStr(mcs, name: str) -> type[QQBatchInterface]:
        """
        Return the batch system class registered with the given name.

        Raises:
            QQError: If no class is registered for the given name.
        """
        if name not in mcs._registry:
            raise QQError(f"No batch system registered for '{name}'.")
        return mcs._registry[name]
