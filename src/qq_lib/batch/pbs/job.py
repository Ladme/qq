# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Self

import yaml

from qq_lib.batch.interface import BatchJobInterface
from qq_lib.batch.pbs.common import parse_pbs_dump_to_dictionary
from qq_lib.core.common import hhmmss_to_duration, load_yaml_dumper
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.size import Size
from qq_lib.properties.states import BatchState

logger = get_logger(__name__)

Dumper: type[yaml.Dumper] = load_yaml_dumper()


class PBSJob(BatchJobInterface):
    """
    Implementation of BatchJobInterface for PBS.
    Stores metadata for a single PBS job.
    """

    def __init__(self, job_id: str):
        self._job_id = job_id
        self._info: dict[str, str] = {}

        self.update()

    def isEmpty(self) -> bool:
        return not self._info

    def getId(self) -> str:
        return self._job_id

    def getAccount(self) -> str | None:
        return None

    def update(self) -> None:
        # get job info from PBS
        command = f"qstat -fxw {self._job_id}"

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            # if qstat fails, information is empty
            logger.debug(
                f"qstat failed: no information about job '{self._job_id}' is available: {result.stderr.strip()}"
            )
            self._info: dict[str, str] = {}
        else:
            self._info = parse_pbs_dump_to_dictionary(result.stdout)

    def getState(self) -> BatchState:
        if not (state := self._info.get("job_state")):
            return BatchState.UNKNOWN

        # if the job is finished and the return code is not zero, return FAILED
        if state == "F":
            exit_code = self.getExitCode()
            # if exit code does not exist, the job never ran and was likely killed
            if exit_code is None or exit_code != 0:
                return BatchState.FAILED

        return BatchState.fromCode(state)

    def getComment(self) -> str | None:
        return self._info.get("comment")

    def getEstimated(self) -> tuple[datetime, str] | None:
        if not (raw_time := self._info.get("estimated.start_time")):
            logger.debug("No 'estimated.start_time' found.")
            return None

        try:
            time = datetime.strptime(raw_time, CFG.date_formats.pbs)
            # if the estimated start time is in the past, use the current time
            if (current_time := datetime.now()) > time:
                time = current_time
        except Exception as e:
            logger.debug(f"Could not parse 'estimated.start_time': {e}.")
            return None

        if not (raw_vnode := self._info.get("estimated.exec_vnode")):
            logger.debug("No 'estimated.exec_vnode' found.")
            return None

        vnodes = []
        for split in raw_vnode.split("+"):
            vnodes.append(PBSJob._cleanNodeName(split.strip()))

        return (time, " + ".join(vnodes))

    def getMainNode(self) -> str | None:
        if raw_node := self._info.get("exec_host2"):
            return PBSJob._cleanNodeName(raw_node.split("+")[0].strip())

        return None

    def getNodes(self) -> list[str] | None:
        if not (raw_nodes := self._info.get("exec_host2")):
            return None

        nodes = []
        for node in raw_nodes.split("+"):
            nodes.append(PBSJob._cleanNodeName(node.strip()))

        return nodes

    def getShortNodes(self) -> list[str] | None:
        if not (raw_nodes := self._info.get("exec_host")):
            return None

        nodes = []
        for node in raw_nodes.split("+"):
            nodes.append(PBSJob._cleanNodeName(node.strip()))

        return nodes

    def getName(self) -> str | None:
        return self._info.get("Job_Name")

    def getNCPUs(self) -> int | None:
        return self._getIntProperty("Resource_List.ncpus", "the number of CPUs")

    def getNGPUs(self) -> int | None:
        return self._getIntProperty("Resource_List.ngpus", "the number of GPUs")

    def getNNodes(self) -> int | None:
        return self._getIntProperty("Resource_List.nodect", "the number of nodes")

    def getMem(self) -> Size | None:
        if not (mem := self._info.get("Resource_List.mem")):
            logger.debug(
                f"Could not get information about the amount of memory from the batch system for '{self._job_id}'."
            )
            return None

        try:
            return Size.fromString(mem)
        except Exception as e:
            logger.warning(f"Could not parse memory for '{self._job_id}': {e}.")
            return None

    def getStartTime(self) -> datetime | None:
        return self._getDatetimeProperty("stime", "the job start time")

    def getSubmissionTime(self) -> datetime | None:
        return self._getDatetimeProperty("ctime", "the job submission time")

    def getCompletionTime(self) -> datetime | None:
        return self._getDatetimeProperty("obittime", "the job completion time")

    def getModificationTime(self) -> datetime | None:
        return (
            self._getDatetimeProperty("mtime", "the job modification time")
            or self.getSubmissionTime()
        )

    def getUser(self) -> str | None:
        if not (user := self._info.get("Job_Owner")):
            return None

        return user.split("@")[0]

    def getWalltime(self) -> timedelta | None:
        if not (walltime := self._info.get("Resource_List.walltime")):
            return None

        try:
            return hhmmss_to_duration(walltime)
        except QQError as e:
            logger.warning(f"Could not parse walltime for '{self._job_id}': {e}.")
            return None

    def getQueue(self) -> str | None:
        return self._info.get("queue")

    def getUtilCPU(self) -> int | None:
        if not (util_cpu := self._info.get("resources_used.cpupercent")):
            logger.debug(
                f"Information about CPU utilization is not available for '{self._job_id}'."
            )
            return None

        if not (ncpus := self.getNCPUs()):
            logger.debug(
                f"Information about the number of CPUs is not available for '{self._job_id}'."
            )
            return None

        try:
            # PBS report CPU utilization in the same way as `top` - we have to divide by number of CPUs
            return int(util_cpu) // ncpus
        except Exception as e:
            # this catches both invalid util_cpu and invalid getNCPUs
            logger.warning(
                f"Could not parse information about CPU utilization for '{self._job_id}': {e}."
            )
            return None

    def getUtilMem(self) -> int | None:
        if not (util_mem := self._info.get("resources_used.mem")):
            logger.debug(
                f"Information about memory utilization is not available for '{self._job_id}'."
            )
            return None

        if not (mem := self.getMem()):
            logger.debug(
                f"Information about the amount of memory is not available for '{self._job_id}'."
            )
            return None

        try:
            util_mem_kb = Size.fromString(util_mem).value
            return int(util_mem_kb / mem.value * 100.0)
        except Exception as e:
            logger.warning(
                f"Could not parse information about memory utilization for '{self._job_id}': {e}."
            )
            return None

    def getExitCode(self) -> int | None:
        if not (exit := self._info.get("Exit_status")):
            return None

        try:
            return int(exit)
        except Exception as e:
            logger.warning(f"Could not parse exit code for '{self._job_id}': {e}.")
            return None

    def getInputMachine(self) -> str | None:
        return self._info.get("Submit_Host")

    def getInputDir(self) -> Path | None:
        if not (env_vars := self._getEnvVars()):
            logger.debug(
                f"Could not get list of environment variables for '{self._job_id}'."
            )
            return None

        if not (
            input_dir := env_vars.get(CFG.env_vars.input_dir)  # try qq first
            or env_vars.get(
                "PBS_O_WORKDIR"
            )  # if this fails, try PBS (note that PBS_O_WORKDIR is not the directory with the submitted script,
            # but the directory from which submission was done)
            or env_vars.get("INF_INPUT_DIR")  # if this fails, try Infinity
        ):
            logger.debug(f"Could not obtain input directory for '{self._job_id}'.")
            return None

        return Path(input_dir).resolve()

    def getInfoFile(self) -> Path | None:
        if not (env_vars := self._getEnvVars()):
            logger.debug(
                f"Could not get list of environment variables for '{self._job_id}'."
            )
            return None

        if not (info_file := env_vars.get(CFG.env_vars.info_file)):
            logger.debug(
                f"Job '{self._job_id}' does not have an assigned qq info file."
            )
            return None

        return Path(info_file)

    def toYaml(self) -> str:
        # we need to add job id to the start of the dictionary
        to_dump = {"Job Id": self._job_id} | self._info
        return yaml.dump(
            to_dump, default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    def getSteps(self) -> list[Self]:
        # not available for PBS
        return []

    def getStepId(self) -> str | None:
        # no job steps for PBS
        return None

    @classmethod
    def fromDict(cls, job_id: str, info: dict[str, str]) -> Self:
        """
        Construct a new instance of PBSJob from a job ID and a dictionary of job information.

        This method bypasses the standard initializer and directly sets the `_job_id` and `_info`
        attributes of the new instance.

        Args:
            job_id (str): The unique identifier of the job.
            info (dict[str, str]): A dictionary containing PBS job metadata as key-value pairs.

        Returns:
            Self: A new instance of PBSJob.

        Note:
            This method does not perform any validation or processing of the provided dictionary.
        """
        job_info = cls.__new__(cls)
        job_info._job_id = job_id
        job_info._info = info

        return job_info

    def getIdInt(self) -> int | None:
        """
        Extract the leading numeric portion of the job ID and return it as an integer.

        Returns:
            int | None: The integer value of the leading digits in the job ID,
            or `None` if no valid digits are found or conversion fails.
        """
        match = re.match(r"\d+", self.getId())
        return int(match.group()) if match else None

    def _getEnvVars(self) -> dict[str, str] | None:
        """
        Retrieve environment variables associated with the job.

        Returns:
            dict[str, str] | None: A dictionary of environment variables, or None
            if no variable list is available.
        """
        if not (variable_list := self._info.get("Variable_List")):
            return None

        return dict(
            item.split("=", 1) for item in variable_list.split(",") if "=" in item
        )

    def _getIntProperty(self, property: str, property_name: str) -> int | None:
        """
        Retrieve an integer property value from the job information.

        If the property is missing or cannot be converted, `None` is returned.

        Args:
            property (str): The key identifying the property in the job information.
            property_name (str): A human-readable name of the property for logging.

        Returns:
            int | None: The integer value of the property, or `None` if unavailable or invalid.
        """
        try:
            return int(self._info[property])
        except Exception:
            logger.debug(
                f"Could not get information about {property_name} from the batch system for '{self._job_id}'."
            )
            return None

    def _getDatetimeProperty(
        self, property: str, property_name: str
    ) -> datetime | None:
        """
        Retrieve and parse a datetime property from the job information.

        Args:
            property (str): The key identifying the property in the job information.
            property_name (str): A human-readable name of the property for logging.

        Returns:
            datetime | None: A datetime object if parsing succeeds, otherwise None.
        """
        if not (raw_datetime := self._info.get(property)):
            return None

        try:
            return datetime.strptime(raw_datetime, CFG.date_formats.pbs)
        except Exception:
            logger.warning(
                f"Could not parse information about {property_name} for '{self._job_id}'."
            )
            return None

    @staticmethod
    def _cleanNodeName(raw: str) -> str:
        """
        Normalize a raw node string to extract the clean hostname.

        Args:
            raw (str): Raw node string reported by the batch system.

        Returns:
            str: Cleaned node name.
        """
        return raw.split(":", 1)[0].split("/", 1)[0].replace("(", "").replace(")", "")
