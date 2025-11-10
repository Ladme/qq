# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import subprocess
from typing import Self

import yaml

from qq_lib.batch.interface.node import BatchNodeInterface
from qq_lib.batch.slurm.common import parse_slurm_dump_to_dictionary
from qq_lib.core.common import load_yaml_dumper
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.size import Size

logger = get_logger(__name__)
Dumper: type[yaml.Dumper] = load_yaml_dumper()


class SlurmNode(BatchNodeInterface):
    """
    Implementation of BatchNodeInterface for Slurm.
    Stores metadata for a single Slurm node.
    """

    def __init__(self, name: str):
        self._name = name
        self._info: dict[str, str] = {}

        self.update()

    def update(self) -> None:
        # get node info from Slurm
        command = f"scontrol show node {self._name} -o"

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

        self._info = parse_slurm_dump_to_dictionary(result.stdout)

    def getName(self) -> str:
        return self._name

    def getNCPUs(self) -> int:
        return self._getIntResource("CPUTot")

    def getNFreeCPUs(self) -> int:
        return self.getNCPUs() - self._getIntResource("CPUAlloc")

    def getNGPUs(self) -> int:
        return self._getIntFromTres("CfgTRES", "gpu")

    def getNFreeGPUs(self) -> int:
        return self.getNGPUs() - self._getIntFromTres("AllocTRES", "gpu")

    def getCPUMemory(self) -> Size:
        # RealMemory corresponds to memory configured in slurm.conf
        return self._getSizeResource("RealMemory")

    def getFreeCPUMemory(self) -> Size:
        # we do not use the FreeMem property as it corresponds to the ACTUAL free memory on the machine
        # and can be higher than RealMemory - AllocMem (e.g. if the jobs don't use all the allocated memory)
        return self.getCPUMemory() - self._getSizeResource("AllocMem")

    def getGPUMemory(self) -> Size:
        return Size(0, "kb")

    def getFreeGPUMemory(self) -> Size:
        return Size(0, "kb")

    def getLocalScratch(self) -> Size:
        return self._getSizeResource("TmpDisk")

    def getFreeLocalScratch(self) -> Size:
        return self._getSizeResource("TmpDisk")

    def getSSDScratch(self) -> Size:
        return Size(0, "kb")

    def getFreeSSDScratch(self) -> Size:
        return Size(0, "kb")

    def getSharedScratch(self) -> Size:
        return Size(0, "kb")

    def getFreeSharedScratch(self) -> Size:
        return Size(0, "kb")

    def getProperties(self) -> list[str]:
        if not (raw := self._info.get("AvailableFeatures")):
            return []

        return raw.split(",")

    def isAvailableToUser(self, user: str) -> bool:
        _ = user

        if not (state := self._info.get("State")):
            logger.debug(f"Could not get state information for node '{self._name}'.")
            return False

        invalid_states = [
            "DOWN",
            "DRAINED",
            "FAIL",
            "FUTURE",
            "INVAL",
            "MAINT",
            "PERFCTRS",
            "POWERED_DOWN",
            "POWERING_DOWN",
            "RESERVED",
            "UNKNOWN",
        ]

        return state not in invalid_states

    @classmethod
    def fromDict(cls, name: str, info: dict[str, str]) -> Self:
        """
        Construct a new instance of SlurmNode from node name and a dictionary of node information.

        Args:
            name (str): The unique name of the node.
            info (dict[str, str]): A dictionary containing Slurm node metadata as key-value pairs.

        Returns:
            Self: A new instance of SlurmNode.

        Note:
            This method does not perform any validation or processing of the provided dictionary.
        """
        node = cls.__new__(cls)
        node._name = name
        node._info = info

        return node

    def toYaml(self) -> str:
        return yaml.dump(
            self._info, default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    def _getIntResource(self, res: str) -> int:
        """
        Retrieve an integer-valued resource from the node information.

        Args:
            res (str): The resource key to retrieve.

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

    def _getIntFromTres(self, tres_key: str, res: str) -> int:
        tres = self._info.get(tres_key, "")

        for item in tres.split(","):
            if res in item:
                try:
                    return int(item.split("=")[1])
                except ValueError as e:
                    logger.debug(
                        f"Could not parse the property '{res}' from '{item}': {e}."
                    )

        return 0

    def _getSizeResource(self, res: str) -> Size:
        if not (val := self._info.get(res)):
            return Size(0, "kb")

        try:
            if val.isnumeric():
                val += "M"
            return Size.fromString(val)
        except Exception as e:
            logger.debug(f"Could not parse the value '{val}' of resource '{res}': {e}.")
            return Size(0, "kb")
