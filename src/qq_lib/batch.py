# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from abc import ABC, ABCMeta, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Self

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


@dataclass
class BatchOperationResult:
    """Class representing the result of a batch system operation.

    Attributes:
        exit_code (int): Exit code of the operation. 0 indicates success.
        success_message (str | None): Optional message returned on success.
        error_message (str | None): Optional message returned on error.
    """

    # exit code of the operation
    exit_code: int

    # optional message in case of a success
    success_message: str | None = None

    # optional message in case of an error
    error_message: str | None = None

    @classmethod
    def error(cls, code: int, msg: str | None = None) -> Self:
        """
        Create a BatchOperationResult representing a failure.

        Args:
            code (int): Non-zero exit code representing the error.
            msg (str | None): Optional error message describing the failure.

        Returns:
            BatchOperationResult: Instance representing an error.
        """
        return cls(exit_code=code, error_message=msg)

    @classmethod
    def success(cls, msg: str | None = None) -> Self:
        """
        Create a BatchOperationResult representing a success.

        Args:
            msg (str | None): Optional message describing the success.

        Returns:
            BatchOperationResult: Instance representing success with exit_code 0.
        """
        return cls(exit_code=0, success_message=msg)

    @classmethod
    def fromExitCode(
        cls,
        code: int,
        success_message: str | None = None,
        error_message: str | None = None,
    ) -> Self:
        """
        Create a BatchOperationResult instance based on an exit code.

        Args:
            code (int): Exit code of the operation. 0 indicates success.
            success_message (str | None): Optional message if the operation succeeded.
            error_message (str | None): Optional message if the operation failed.

        Returns:
            BatchOperationResult: Success or error instance depending on the exit code.
        """
        return (
            cls.success(success_message)
            if code == 0
            else cls.error(code, error_message)
        )


class QQBatchInterface[TBatchInfo: BatchJobInfoInterface](ABC):
    """
    Abstract base class for batch system integrations.

    Concrete batch system classes must implement these methods to allow
    qq to interact with different batch systems uniformly.

    All methods are static and should never raise exceptions!
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
    def getScratchDir(job_id: str) -> BatchOperationResult:
        """
        Retrieve the scratch directory for a given job.

        Args:
            job_id (int): Unique identifier of the job.

        Returns:
            BatchOperationResult: Result of the operation.
                                  Success message must contain path to the
                                  scratch directory as a string.
                                  Error message can contain anything reasonable.
        """
        pass

    @staticmethod
    @abstractmethod
    def jobSubmit(res: QQResources, queue: str, script: Path) -> BatchOperationResult:
        """
        Submit a job to the batch system.

        Args:
            res (QQResources): Resources required for the job.
            queue (str): Target queue for the job submission.
            script (Path): Path to the script to execute.

        Returns:
            BatchOperationResult: Result of the submission.
                                  Success message must contain the job id.
                                  Error message should contain stderr of the command.
        """
        pass

    @staticmethod
    @abstractmethod
    def jobKill(job_id: str) -> BatchOperationResult:
        """
        Terminate a job gracefully. This assumes that job has time for cleanup.

        Args:
            job_id (str): Identifier of the job to terminate.

        Returns:
            BatchOperationResult: Result of the kill operation.
                                  Success message is unused.
                                  Error message should contain stderr of the command.
        """
        pass

    @staticmethod
    @abstractmethod
    def jobKillForce(job_id: str) -> BatchOperationResult:
        """
        Forcefully terminate a job. This assumes that the job has no time for cleanup.

        Args:
            job_id (str): Identifier of the job to forcefully terminate.

        Returns:
            BatchOperationResult: Result of the forced kill operation.
                                  Success message is unused.
                                  Error message should contains stderr of the command.
        """
        pass

    @staticmethod
    @abstractmethod
    def navigateToDestination(host: str, directory: Path) -> BatchOperationResult:
        """
        Navigate to a directory on the specified host.

        Args:
            host (str): Target hostname where the directory resides.
            directory (Path): Path to navigate to.

        Returns:
            BatchOperationResult: Result of the operation.
                                  Both success and error message are unused.
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
