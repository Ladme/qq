# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from abc import ABC, abstractmethod
from pathlib import Path
from subprocess import CompletedProcess

from qq_lib.resources import QQResources


class QQBatchInterface(ABC):
    """
    Abstract base class for batch system classes.

    Defines the methods that concrete batch system classes must implement.
    """

    @staticmethod
    @abstractmethod
    def envName() -> str:
        """
        Return the name of the batch system environment.

        Returns:
            str: The batch system name (e.g., "PBS", "SLURM").
        """
        pass

    @staticmethod
    @abstractmethod
    def usernameEnvVar() -> str:
        """
        Return the environment variable name that stores the username.

        Returns:
            str: Environment variable name for the user name.
        """
        pass

    @staticmethod
    @abstractmethod
    def jobIdEnvVar() -> str:
        """
        Return the environment variable name that stores the job ID.

        Returns:
            str: Environment variable name for the job ID.
        """
        pass

    @staticmethod
    @abstractmethod
    def workDirEnvVar() -> str:
        """
        Return the environment variable name that stores the current work directory.

        Returns
            str: Environment variable name for the workdir.
        """
        pass

    @staticmethod
    @abstractmethod
    def translateSubmit(res: QQResources, queue: str, script: str) -> str:
        pass

    @staticmethod
    @abstractmethod
    def translateKill(job_id: str) -> str:
        pass

    @staticmethod
    @abstractmethod
    def navigateToDestination(host: str, directory: Path) -> CompletedProcess[bytes]:
        pass
