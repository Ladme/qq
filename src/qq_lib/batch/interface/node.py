# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from abc import ABC, abstractmethod

from qq_lib.properties.size import Size


class BatchNodeInterface(ABC):
    """
    Abstract base class for obtaining information about compute nodes.

    The implementation of the constructor is arbitrary and should only
    be used inside the corresponding implementation of `BatchInterface.getNodes`.
    """

    @abstractmethod
    def update(self) -> None:
        """
        Refresh the stored node information from the batch system.

        Raises:
            QQError: If the node cannot be queried or its info updated.
        """
        pass

    @abstractmethod
    def getName(self) -> str:
        """
        Retrieve the name of the node.

        Returns:
            str: The name identifying the node in the batch system.
        """
        pass

    @abstractmethod
    def getNCPUs(self) -> int | None:
        """
        Retrieve the total number of CPU cores available on the node.

        Returns:
            int | None: Total CPU core count or `None` if not available.
        """
        pass

    @abstractmethod
    def getNFreeCPUs(self) -> int | None:
        """
        Retrieve the number of currently available (unallocated) CPU cores.

        Returns:
            int | None: Number of free CPU cores or `None` if not available.
        """
        pass

    @abstractmethod
    def getNGPUs(self) -> int | None:
        """
        Retrieve the total number of GPUs available on the node.

        Returns:
            int | None: Total GPU count or `None` if not available..
        """
        pass

    @abstractmethod
    def getNFreeGPUs(self) -> int | None:
        """
        Retrieve the number of currently available (unallocated) GPUs.

        Returns:
            int | None: Number of free GPUs or `None` if not available.
        """
        pass

    @abstractmethod
    def getCPUMemory(self) -> Size | None:
        """
        Retrieve the total CPU memory capacity of the node.

        Returns:
            Size | None: Total CPU memory available on the node or `None` if not available.
        """
        pass

    @abstractmethod
    def getFreeCPUMemory(self) -> Size | None:
        """
        Retrieve the currently available CPU memory.

        Returns:
            Size | None: Free (unused) CPU memory or `None` if not available.
        """
        pass

    @abstractmethod
    def getGPUMemory(self) -> Size | None:
        """
        Retrieve the total GPU memory capacity of the node.

        Returns:
            Size | None: Total GPU memory available or `None` if not available.
        """
        pass

    @abstractmethod
    def getFreeGPUMemory(self) -> Size | None:
        """
        Retrieve the currently available GPU memory.

        Returns:
            Size | None: Free (unused) GPU memory or `None` if not available.
        """
        pass

    @abstractmethod
    def getLocalScratch(self) -> Size | None:
        """
        Retrieve the total local scratch storage capacity of the node.

        Returns:
            Size | None: Total size of local scratch space or `None` if not available.
        """
        pass

    @abstractmethod
    def getFreeLocalScratch(self) -> Size | None:
        """
        Retrieve the available local scratch storage space.

        Returns:
            Size | None: Free local scratch space or `None` if not available.
        """
        pass

    @abstractmethod
    def getSSDScratch(self) -> Size | None:
        """
        Retrieve the total SSD-based scratch storage capacity.

        Returns:
            Size | None: Total SSD scratch capacity or `None` if not available.
        """
        pass

    @abstractmethod
    def getFreeSSDScratch(self) -> Size | None:
        """
        Retrieve the currently available SSD-based scratch storage space.

        Returns:
            Size | None: Free SSD scratch space or `None` if not available.
        """
        pass

    @abstractmethod
    def getSharedScratch(self) -> Size | None:
        """
        Retrieve the total capacity of shared scratch storage accessible from the node.

        Returns:
            Size | None: Total shared scratch capacity or `None` if not available.
        """
        pass

    @abstractmethod
    def getFreeSharedScratch(self) -> Size | None:
        """
        Retrieve the available space in shared scratch storage.

        Returns:
            Size | None: Free shared scratch space or `None` if not available.
        """
        pass

    @abstractmethod
    def getProperties(self) -> list[str]:
        """
        Get the list of properties or labels assigned to the node.

        Returns:
            list[str]: List of node property strings.
        """
        pass

    @abstractmethod
    def isAvailableToUser(self, user: str) -> bool:
        """
        Check if the node is available to the specified user.

        Args:
            user (str): The username to check access for.

        Returns:
            bool: True if the node is up and schedulable, False otherwise.
        """
        pass

    @abstractmethod
    def toYaml(self) -> str:
        """
        Return all information about the node in YAML format.

        Returns:
            str: YAML-formatted string of node metadata.
        """
        pass
