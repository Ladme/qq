# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from abc import ABC, ABCMeta, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, Self, TypeVar

from qq_lib.error import QQError
from qq_lib.resources import QQResources
from qq_lib.states import BatchState

# forward declaration
class BatchJobInfoInterface(ABC): 
    pass  

@dataclass
class BatchOperationResult:
    """Class reporting results of batch system operations."""
    # exit code of the operation
    exit_code: int

    # optional message in case of a success
    success_message: str | None = None

    # optional message in case of an error
    error_message: str | None = None

    @classmethod
    def error(cls, code: int, msg: str | None = None) -> Self:
        return cls(exit_code = code, error_message = msg)

    @classmethod
    def success(cls, msg: str | None = None) -> Self:
        return cls(exit_code = 0, success_message = msg)
    
    @classmethod
    def fromExitCode(cls, code: int, success_message: str | None = None, error_message: str | None = None) -> Self:
        """Create a BatchOperationResult instance based on an exit code."""
        return cls.success(success_message) if code == 0 else cls.error(code, error_message)

TBatchInfo = TypeVar("TBatchInfo", bound=BatchJobInfoInterface)

class QQBatchInterface(ABC, Generic[TBatchInfo]):
    """
    Abstract base class for batch system classes.

    Defines the methods that concrete batch system classes must implement.

    Note that methods of QQBatchInterface should never raise!
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
    def getScratchDir(job_id: int) -> BatchOperationResult:
        pass

    @staticmethod
    @abstractmethod
    def jobSubmit(res: QQResources, queue: str, script: Path) -> BatchOperationResult:
        pass

    @staticmethod
    @abstractmethod
    def jobKill(job_id: str) -> BatchOperationResult:
        pass

    @staticmethod
    @abstractmethod
    def jobKillForce(job_id: str) -> BatchOperationResult:
        pass

    @staticmethod
    @abstractmethod
    def navigateToDestination(host: str, directory: Path) -> BatchOperationResult:
        pass

    @staticmethod
    @abstractmethod
    def getJobInfo(job_id: str) -> TBatchInfo:
        """
        Retrieve information about a job from the batch system.

        The returned object must be fully initialized even if the job
        information is no longer available.

        This method should never raise.
        """
        pass

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
        print(mcs._registry)
        if name not in mcs._registry:
            raise QQError(f"No batch system registered for '{name}'.")
        return mcs._registry[name]
