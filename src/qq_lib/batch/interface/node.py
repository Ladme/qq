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
    def getNCPUs(self) -> int:
        """
        Retrieve the total number of CPU cores available on the node.

        Returns:
            int: Total CPU core count.
        """
        pass

    @abstractmethod
    def getNFreeCPUs(self) -> int:
        """
        Retrieve the number of currently available (unallocated) CPU cores.

        Returns:
            int: Number of free CPU cores.
        """
        pass

    @abstractmethod
    def getNGPUs(self) -> int:
        """
        Retrieve the total number of GPUs available on the node.

        Returns:
            int: Total GPU count.
        """
        pass

    @abstractmethod
    def getNFreeGPUs(self) -> int:
        """
        Retrieve the number of currently available (unallocated) GPUs.

        Returns:
            int: Number of free GPUs.
        """
        pass

    @abstractmethod
    def getCPUMemory(self) -> Size:
        """
        Retrieve the total CPU memory capacity of the node.

        Returns:
            Size: Total CPU memory available on the node.
        """
        pass

    @abstractmethod
    def getFreeCPUMemory(self) -> Size:
        """
        Retrieve the currently available CPU memory.

        Returns:
            Size: Free (unused) CPU memory.
        """
        pass

    @abstractmethod
    def getGPUMemory(self) -> Size:
        """
        Retrieve the total GPU memory capacity of the node.

        Returns:
            Size: Total GPU memory available.
        """
        pass

    @abstractmethod
    def getFreeGPUMemory(self) -> Size:
        """
        Retrieve the currently available GPU memory.

        Returns:
            Size: Free (unused) GPU memory.
        """
        pass

    @abstractmethod
    def getLocalScratch(self) -> Size:
        """
        Retrieve the total local scratch storage capacity of the node.

        Returns:
            Size: Total size of local scratch space.
        """
        pass

    @abstractmethod
    def getFreeLocalScratch(self) -> Size:
        """
        Retrieve the available local scratch storage space.

        Returns:
            Size: Free local scratch space.
        """
        pass

    @abstractmethod
    def getSSDScratch(self) -> Size:
        """
        Retrieve the total SSD-based scratch storage capacity.

        Returns:
            Size: Total SSD scratch capacity.
        """
        pass

    @abstractmethod
    def getFreeSSDScratch(self) -> Size:
        """
        Retrieve the currently available SSD-based scratch storage space.

        Returns:
            Size: Free SSD scratch space.
        """
        pass

    @abstractmethod
    def getSharedScratch(self) -> Size:
        """
        Retrieve the total capacity of shared scratch storage accessible from the node.

        Returns:
            Size: Total shared scratch capacity.
        """
        pass

    @abstractmethod
    def getFreeSharedScratch(self) -> Size:
        """
        Retrieve the available space in shared scratch storage.

        Returns:
            Size: Free shared scratch space.
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
