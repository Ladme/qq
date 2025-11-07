# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Self

import yaml

from qq_lib.batch.interface.job import BatchJobInterface
from qq_lib.core.common import dhhmmss_to_duration, load_yaml_dumper
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.size import Size
from qq_lib.properties.states import BatchState

from .common import SACCT_FIELDS, parse_slurm_dump_to_dictionary

logger = get_logger(__name__)

Dumper: type[yaml.Dumper] = load_yaml_dumper()


class SlurmJob(BatchJobInterface):
    """
    Implementation of BatchJobInterface for Slurm.
    Stores metadata for a single Slurm job.
    """

    STATE_CONVERTER: dict[str, BatchState] = {
        "BOOT_FAIL": BatchState.FAILED,
        "CANCELLED": BatchState.FAILED,
        "COMPLETED": BatchState.FINISHED,
        "DEADLINE": BatchState.FAILED,
        "FAILED": BatchState.FAILED,
        "NODE_FAIL": BatchState.FAILED,
        "OUT_OF_MEMORY": BatchState.FAILED,
        "PENDING": BatchState.QUEUED,
        "PREEMPTED": BatchState.SUSPENDED,
        "RUNNING": BatchState.RUNNING,
        "SUSPENDED": BatchState.SUSPENDED,
        "TIMEOUT": BatchState.FAILED,
    }

    def __init__(self, job_id: str):
        self._job_id = job_id
        self._info: dict[str, str] = {}

        self.update()

    def isEmpty(self) -> bool:
        return not self._info

    def getId(self) -> str:
        return self._job_id

    def getAccount(self) -> str | None:
        return self._info.get("Account")

    def update(self) -> None:
        # first try `scontrol`
        command = f"scontrol show job {self._job_id} -o"
        logger.debug(command)

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            # if scontrol fails, try sacct
            logger.debug(
                f"scontrol failed for job '{self._job_id}' ({result.stderr.strip()}); trying sacct"
            )
        else:
            self._info: dict[str, str] = parse_slurm_dump_to_dictionary(result.stdout)
            return

        # if `scontrol` fails, try `sacct`
        command = f"sacct --allocations --noheader --parsable2 -j {self._job_id} --format={SACCT_FIELDS} "
        logger.debug(command)

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            # if sacct fails, information is empty
            logger.debug(
                f"both scontrol and sacct failed: no information about job '{self._job_id}' is available: {result.stderr.strip()}"
            )
            self._info: dict[str, str] = {}
        else:
            job: SlurmJob = SlurmJob.fromSacctString(result.stdout.strip())
            self._info: dict[str, str] = job._info

    def getState(self) -> BatchState:
        if not (raw_state := self._info.get("JobState")):
            return BatchState.UNKNOWN

        converted_state = SlurmJob.STATE_CONVERTER.get(raw_state) or BatchState.UNKNOWN

        # if the job is queued due to depending on another job, it should be considered "held"
        if (
            converted_state == BatchState.QUEUED
            and (comment := self.getComment())
            and "Dependency" in comment
        ):
            return BatchState.HELD

        return converted_state

    def getComment(self) -> str | None:
        if (reason := self._info.get("Reason")) and reason != "None":
            return f"Reason: {reason}"

        return None

    def getEstimated(self) -> tuple[datetime, str] | None:
        # use "StartTime" as an estimate
        if not (time := self.getStartTime()) or time == "None":
            return None

        if not (node_list := self._info.get("SchedNodeList")) or "None" in node_list:
            return None

        return (time, node_list)

    def getMainNode(self) -> str | None:
        if (main_node := self._info.get("BatchHost")) and "None" not in main_node:
            return main_node

        # if BatchHost does not exist, use the first node from NodeList
        if nodes := self.getNodes():
            return nodes[0]

        return None

    def getNodes(self) -> list[str] | None:
        if (node_list := self._info.get("NodeList")) and "None" not in node_list:
            return SlurmJob._expandNodeList(node_list)

        return None

    def getShortNodes(self) -> list[str] | None:
        # treat all nodes a single node, without expanding
        # this assumes that getShortNodes is only used in qq jobs and qq stat
        if (node_list := self._info.get("NodeList")) and "None" not in node_list:
            return [node_list]

        return None

    def getName(self) -> str:
        if not (name := self._info.get("JobName")):
            logger.warning(f"Could not get job name for '{self._job_id}'.")
            return "?????"

        return name

    def getNCPUs(self) -> int:
        return self._getIntProperty("NumCPUs", "the number of CPUs")

    def getNGPUs(self) -> int:
        tres = self._getTres()
        for item in tres.split(","):
            if item.startswith("gpu=") or item.startswith("gres/gpu="):
                try:
                    return int(item.split("=")[1])
                except ValueError as e:
                    logger.debug(
                        f"Could not parse the number of GPUs from '{item}': {e}."
                    )

        return 0

    def getNNodes(self) -> int:
        return self._getIntProperty("NumNodes", "the number of nodes")

    def getMem(self) -> Size:
        tres = self._getTres()
        for item in tres.split(","):
            if item.startswith("mem="):
                try:
                    return Size.fromString(item.split("=", 1)[1])
                except Exception as e:
                    logger.warning(f"Could not parse memory for '{self._job_id}': {e}.")
                    return Size(0, "kb")

        logger.warning(f"Memory not available for '{self._job_id}'.")
        return Size(0, "kb")

    def getStartTime(self) -> datetime | None:
        return self._getDatetimeProperty("StartTime", "the job start time")

    def getSubmissionTime(self) -> datetime:
        return (
            self._getDatetimeProperty("SubmitTime", "the job submission time")
            or datetime.min  # arbitrary datetime - submission time should always be available
        )

    def getCompletionTime(self) -> datetime | None:
        # the property EndTime is available for running jobs as well (estimated completion time)
        # but that should not matter for our purposes
        return self._getDatetimeProperty("EndTime", "the job completion time")

    def getModificationTime(self) -> datetime:
        # assuming this is only used for completed jobs
        return self.getCompletionTime() or self.getSubmissionTime()

    def getUser(self) -> str:
        if not (user := self._info.get("UserId")):
            logger.warning(f"Could not get user for '{self._job_id}'.")
            return "?????"

        return user.split("(")[0]

    def getWalltime(self) -> timedelta:
        if not (walltime := self._info.get("TimeLimit")):
            logger.warning(f"Could not get walltime for '{self._job_id}'.")
            return timedelta(0)

        try:
            return dhhmmss_to_duration(walltime)
        except QQError as e:
            logger.warning(f"Could not parse walltime for '{self._job_id}': {e}.")
            return timedelta(0)

    def getQueue(self) -> str:
        if not (queue := self._info.get("Partition")):
            logger.warning(f"Could not get queue for '{self._job_id}'.")
            return "?????"

        return queue

    def getUtilCPU(self) -> int | None:
        # not available in Slurm
        return None

    def getUtilMem(self) -> int | None:
        # not available in Slurm
        return None

    def getExitCode(self) -> int | None:
        if not (raw_exit := self._info.get("ExitCode")):
            return None

        try:
            # Slurm reports two exit codes; the first one is exit code of the script
            # the second one is a signal
            # we return the first non-zero exit code or 0 if both exit codes are 0
            code, signal = map(int, raw_exit.split(":"))
            return code or signal
        except Exception as e:
            logger.debug(f"Could not parse exit codes '{raw_exit}': {e}.")
            return None

    def getInputMachine(self) -> str:
        # not available for Slurm
        return "?????"

    def getInputDir(self) -> Path:
        if not (raw_dir := self._info.get("WorkDir")):
            logger.warning(f"Could not obtain input directory for '{self._job_id}'.")
            return Path("???")

        return Path(raw_dir).resolve()

    def getInfoFile(self) -> Path | None:
        info_file = (self.getInputDir() / self.getName()).with_suffix(
            CFG.suffixes.qq_info
        )

        # we need to check whether the info file actually exists
        # (or rather if it is available to the user)
        try:
            if not info_file.is_file():
                return None
        except PermissionError:
            return None

        return info_file

    def toYaml(self) -> str:
        return yaml.dump(
            self._info, default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    @classmethod
    def fromDict(cls, job_id: str, info: dict[str, str]) -> Self:
        """
        Construct a new instance of SlurmJob from a job ID and a dictionary of job information.

        This method bypasses the standard initializer and directly sets the `_job_id` and `_info`
        attributes of the new instance.

        Args:
            job_id (str): The unique identifier of the job.
            info (dict[str, str]): A dictionary containing Slurm job metadata as key-value pairs.

        Returns:
            Self: A new instance of SlurmJob.

        Note:
            This method does not perform any validation or processing of the provided dictionary.
        """
        job_info = cls.__new__(cls)
        job_info._job_id = job_id
        job_info._info = info

        return job_info

    @classmethod
    def fromSacctString(cls, string: str) -> Self:
        """
        Construct a new instance of SlurmJob using a string from sacct.

        Args:
            string (str): String describing the job properties obtained using sacct.

        Returns:
            Self: A new instance of SlurmJob.
        """
        fields: list[str] = [
            "JobId",
            "Account",
            "JobState",
            "UserId",
            "JobName",
            "Partition",
            "WorkDir",
            "AllocCPUs",
            "ReqCPUs",
            "AllocTRES",
            "ReqTRES",
            "AllocNodes",
            "ReqNodes",
            "SubmitTime",
            "StartTime",
            "EndTime",
            "TimeLimit",
            "NodeList",
            "Reason",
            "ExitCode",
        ]

        split = string.split("|")
        if len(fields) != len(split):
            raise QQError(
                f"Number of items in a sacct string '{string}' ('{len(split)}') does not match the expected number of items ('{len(fields)}'). This is a bug, please report it!"
            )

        info: dict[str, str] = dict(zip(fields, split))

        # only take the first word from JobState
        # other words may contain useless additional information
        info["JobState"] = info["JobState"].split()[0]

        SlurmJob._assignIfAllocated(info, "AllocCPUs", "ReqCPUs", "NumCPUs")
        SlurmJob._assignIfAllocated(info, "AllocNodes", "ReqNodes", "NumNodes")

        return SlurmJob.fromDict(info["JobId"], info)

    def getIdsForSorting(self) -> list[int]:
        """
        Extract numeric components of the job ID for sorting.

        The method retrieves the leading numeric portion of the job ID, which may
        contain multiple integer groups separated by underscores. Parsing stops
        when a non-digit and non-underscore character is encountered.

        Returns:
            list[int]: A list of integer components extracted from the job ID,
                or [0] if no valid numeric portion is found.
        """
        # get the numerical portion of the job ID (may contain underscores)
        match = re.match(r"(\d+(?:_\d+)*)", self.getId())
        if not match:
            return [0]

        # split the matched portion into digit groups
        groups = match.group(1).split("_")
        return [int(g) for g in groups]

    @staticmethod
    def _expandNodeList(compact: str) -> list[str]:
        """
        Expand a compact Slurm node list expression into individual hostnames.

        This method uses the Slurm `scontrol show hostnames` command to translate
        a compact node list (e.g., "node[01-03]") into an explicit list of node names.
        If the expansion fails, the original compact string is returned as a single-element list.

        Args:
            compact (str): The compact Slurm node list expression to expand.

        Returns:
            list[str]: A list of fully expanded node hostnames. If expansion fails,
                returns a list containing the original input string.
        """
        command = f"scontrol show hostnames {compact}"
        logger.debug(command)

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            logger.warning(
                f"Could not expand '{compact}' into a list of nodes: {result.stderr.strip()}"
            )
            # use unexpanded string
            return [compact]

        return result.stdout.strip().split("\n")

    def _getIntProperty(self, property: str, property_name: str) -> int:
        """
        Retrieve an integer property value from the job information.

        If the property contains a range (e.g., "MIN-MAX"), only the minimum value
        is returned. If the property cannot be retrieved or converted to an integer,
        0 is returned by default.

        Args:
            property (str): The key identifying the property in the job information.
            property_name (str): A human-readable name of the property for logging.

        Returns:
            int: The integer value of the property, or 0 if unavailable or invalid.
        """
        try:
            # we split by '-' because pending jobs may have this property shown as MIN-MAX
            # we show the value of the minimum
            return int(self._info[property].split("-")[0])
        except Exception:
            logger.debug(
                f"Could not get information about {property_name} from the batch system for '{self._job_id}'."
            )
            # if not specified, we assume 0
            return 0

    def _getDatetimeProperty(
        self, property: str, property_name: str
    ) -> datetime | None:
        """
        Retrieve and parse a datetime property from the job information.

        If the property is missing, empty, or marked as unknown, None is returned.
        A warning is logged if parsing fails.

        Args:
            property (str): The key identifying the property in the job information.
            property_name (str): A human-readable name of the property for logging.

        Returns:
            datetime | None: A datetime object if parsing succeeds, otherwise None.
        """
        if not (raw_datetime := self._info.get(property)) or raw_datetime.lower() in [
            "unknown",
            "n/a",
            "none",
            "",
        ]:
            return None

        try:
            return datetime.strptime(raw_datetime, CFG.date_formats.slurm)
        except Exception as e:
            logger.warning(
                f"Could not parse information about {property_name} for '{self._job_id}': {e}."
            )
            return None

    def _getTres(self) -> str:
        """
        Return the AllocTRES property or ReqTRES property, depending on which of them is available.
        Note that the resources specified in ReqTRES can potentially be different than the resources in AllocTRES.
        """
        tres = self._info.get("AllocTRES")
        if not tres or "null" in tres or "None" in tres or "N/A" in tres:
            tres = self._info.get("ReqTRES", "")

        return tres

    @staticmethod
    def _assignIfAllocated(
        info: dict[str, str], alloc_key: str, req_key: str, target_key: str
    ) -> None:
        """
        Assigns a value to a target key in the `info` dictionary, preferring an allocated value
        if it exists and is valid; otherwise, falls back to the requested value.

        Args:
            info (dict[str, str]): The dictionary containing allocation and request data.
            alloc_key (str): The key for the allocated resource (e.g., "AllocCPUs").
            req_key (str): The key for the requested resource (e.g., "ReqCPUs").
            target_key (str): The key under which the resolved value should be stored (e.g., "NumCPUs").

        Notes:
            - A value is considered invalid if it is `None`, an empty string `""`, or `"0"`.
            - The function updates `info` in place.
        """
        value = info.get(alloc_key)
        info[target_key] = (
            value if value not in (None, "None", "", "0") else info.get(req_key, "0")
        )
