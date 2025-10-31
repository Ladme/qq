# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import subprocess
from typing import Self

import yaml

from qq_lib.batch.interface.node import BatchNodeInterface
from qq_lib.batch.pbs.common import parsePBSDumpToDictionary
from qq_lib.batch.pbs.queue import PBSQueue
from qq_lib.core.common import load_yaml_dumper
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.size import Size

logger = get_logger(__name__)

Dumper: type[yaml.Dumper] = load_yaml_dumper()


class QueuesAvailability:
    """
    Utility class for obtaining and caching the availability status of batch queues.
    """

    _queues: dict[str, dict[str, bool]] = {}

    @staticmethod
    def getOrInit(queue: str, user: str) -> bool:
        """
        Retrieve the availability of a queue for the given user.

        Args:
            queue (str): The name of the queue to check.
            user (str): The username to check access for.

        Returns:
            bool:
                True if the queue is available to the current user,
                False otherwise.
        """
        # check whether the availability is cached
        if (avail_dict := QueuesAvailability._queues.get(queue)) is not None and (
            available := avail_dict.get(user)
        ) is not None:
            return available

        # get the availability by querying the batch system
        available = PBSQueue(queue).isAvailableToUser(user)

        # cache the result
        try:
            QueuesAvailability._queues[queue][user] = available
        except KeyError:
            QueuesAvailability._queues[queue] = {user: available}
        logger.debug(f"Initialized availability of '{queue}' for user '{user}'.")
        return available


class PBSNode(BatchNodeInterface):
    """
    Implementation of BatchNodeInterface for PBS.
    Stores metadata for a single PBS node.
    """

    def __init__(self, name: str):
        self._name = name
        self._info: dict[str, str] = {}

        self.update()

    def update(self) -> None:
        # get node info from PBS
        command = f"pbsnodes -v {self._name}"

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(f"Node '{self._name}' does not exist.")

        self._info = parsePBSDumpToDictionary(result.stdout)

    def getName(self) -> str:
        return self._name

    def getNCPUs(self) -> int:
        return self._getIntResource("resources_available.ncpus")

    def getNFreeCPUs(self) -> int:
        return self._getFreeIntResource("ncpus")

    def getNGPUs(self) -> int:
        return self._getIntResource("resources_available.ngpus")

    def getNFreeGPUs(self) -> int:
        return self._getFreeIntResource("ngpus")

    def getCPUMemory(self) -> Size:
        return self._getSizeResource("resources_available.mem")

    def getFreeCPUMemory(self) -> Size:
        return self._getFreeSizeResource("mem")

    def getGPUMemory(self) -> Size:
        return self._getSizeResource("resources_available.gpu_mem")

    def getFreeGPUMemory(self) -> Size:
        return self._getFreeSizeResource("gpu_mem")

    def getLocalScratch(self) -> Size:
        return self._getSizeResource("resources_available.scratch_local")

    def getFreeLocalScratch(self) -> Size:
        return self._getFreeSizeResource("scratch_local")

    def getSSDScratch(self) -> Size:
        return self._getSizeResource("resources_available.scratch_ssd")

    def getFreeSSDScratch(self) -> Size:
        return self._getFreeSizeResource("scratch_ssd")

    def getSharedScratch(self) -> Size:
        return self._getSizeResource("resources_available.scratch_shared")

    def getFreeSharedScratch(self) -> Size:
        return self._getFreeSizeResource("scratch_shared")

    def getProperties(self) -> list[str]:
        return [
            key.split(".", 1)[-1]
            for key in self._info
            if "resources_available" in key and self._info[key] == "True"
        ]

    def isAvailableToUser(self, user: str) -> bool:
        if not (state := self._info.get("state")):
            logger.debug(f"Could not get state information for node '{self._name}'.")
            return False

        if any(
            disabled_state in state
            for disabled_state in {"down", "unknown", "unresolvable", "resv-exclusive"}
        ):
            return False

        if queue := self._info.get("queue"):
            return QueuesAvailability.getOrInit(queue, user)

        return True

    @classmethod
    def fromDict(cls, name: str, info: dict[str, str]) -> Self:
        """
        Construct a new instance of PBSNode from node name and a dictionary of node information.

        Args:
            name (str): The unique name of the node.
            info (dict[str, str]): A dictionary containing PBS node metadata as key-value pairs.

        Returns:
            Self: A new instance of PBSNode.

        Note:
            This method does not perform any validation or processing of the provided dictionary.
        """
        node = cls.__new__(cls)
        node._name = name
        node._info = info

        return node

    def toYaml(self) -> str:
        # we need to add node name to the start of the dictionary
        to_dump = {"Node": self._name} | self._info
        return yaml.dump(
            to_dump, default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    def _getIntResource(self, res: str) -> int:
        """
        Retrieve an integer-valued resource from the node information.

        Args:
            res (str): The resource key to retrieve (e.g., "resources_available.ncpus").

        Returns:
            int: The integer value of the resource, or `0` if unavailable or invalid.
        """
        if not (val := self._info.get(res)):
            return 0
        try:
            return int(val)
        except Exception as e:
            logger.debug(f"Could not parse the value '{val}' of resource '{res}': {e}.")
            return 0

    def _getFreeIntResource(self, res: str) -> int:
        """
        Compute the number of free units for an integer-valued resource.

        Calculates the difference between the total available (`resources_available.<res>`)
        and the assigned (`resources_assigned.<res>`) quantities. If the computed
        difference is negative or invalid, returns `0`.

        Args:
            res (str): The base resource name (e.g., "ncpus", "ngpus").

        Returns:
            int: The number of unallocated (free) resource units, or `0` if unavailable.
        """
        full = self._getIntResource(f"resources_available.{res}")
        assigned = self._getIntResource(f"resources_assigned.{res}")

        if (diff := full - assigned) >= 0:
            return diff

        return 0

    def _getSizeResource(self, res: str) -> Size:
        """
        Retrieve a Size resource from the node information.

        Args:
            res (str): The resource key to retrieve (e.g., "resources_available.mem").

        Returns:
            Size: The parsed Size, or a zero-size object if unavailable or invalid.
        """
        if not (val := self._info.get(res)):
            return Size(0, "kb")

        try:
            return Size.fromString(val)
        except Exception as e:
            logger.debug(f"Could not parse the value '{val}' of resource '{res}': {e}.")
            return Size(0, "kb")

    def _getFreeSizeResource(self, res: str) -> Size:
        """
        Compute the amount of free space for a Size resource.

        Calculates the difference between total available (`resources_available.<res>`)
        and assigned (`resources_assigned.<res>`) values. If subtraction fails or
        results in a negative size, returns a zero-size object.

        Args:
            res (str): The base resource name (e.g., "mem", "scratch_local").

        Returns:
            Size: The available (free) size for the resource, or `0 kb` if unavailable.
        """
        full = self._getSizeResource(f"resources_available.{res}")
        assigned = self._getSizeResource(f"resources_assigned.{res}")

        try:
            return full - assigned
        except ValueError as e:
            logger.debug(f"Negative free size of resource '{res}': {e}.")
            return Size(0, "kb")
