# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
import socket
import subprocess
from datetime import timedelta
from typing import Self

import yaml

from qq_lib.batch.interface.queue import BatchQueueInterface
from qq_lib.core.common import hhmmss_to_duration
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)

# load faster YAML dumper
try:
    from yaml import CDumper as Dumper  # ty: ignore[possibly-unbound-import]

    logger.debug("Loaded YAML CDumper.")
except ImportError:
    from yaml import Dumper

    logger.debug("Loaded default YAML dumper.")


class ACLData:
    """
    Utility class for caching and retrieving access control (ACL) context data.

    Improves performance when multiple ACL checks are performed repeatedly during queue evaluations.
    """

    _groups: dict[str, list[str]] = {}
    _host: str | None = None

    @staticmethod
    def getGroupsOrInit(user: str) -> list[str]:
        """
        Retrieve the cached group memberships for a user, initializing them if needed.
        Args:
            user (str): The username whose group memberships should be retrieved.

        Returns:
            list[str]: A list of group names the user belongs to.
                       Returns an empty list if the system command fails.
        """
        if groups := ACLData._groups.get(user):
            return groups

        result = subprocess.run(
            ["bash"],
            input=f"id -nG {user}",
            text=True,
            check=False,
            capture_output=True,
        )

        if result.returncode != 0:
            ACLData._groups[user] = []
            return []

        groups = result.stdout.split()
        ACLData._groups[user] = groups
        logger.debug(f"Initialized ACL groups for user '{user}': {groups}.")
        return groups

    @staticmethod
    def getHostOrInit() -> str:
        """
        Retrieve the cached hostname, initializing it if not already set.

        Returns:
            str: The local machine's hostname.
        """
        if host := ACLData._host:
            return host

        host = socket.gethostname()
        ACLData._host = host
        logger.debug(f"Initialized ACL host: {host}.")
        return host


class PBSQueue(BatchQueueInterface):
    """
    Implementation of BatchQueueInterface for PBSQueue.
    Stores metadata for a single PBS queue.
    """

    def __init__(self, name: str):
        self._name = name
        self._info: dict[str, str] = {}

        self.update()

    def update(self) -> None:
        # get queue info from PBS
        command = f"qstat -Qfw {self._name}"

        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode != 0:
            raise QQError(f"Queue '{self._name}' does not exist.")

        self._info = PBSQueue._parsePBSDumpToDictionary(result.stdout)  # ty: ignore[possibly-unbound-attribute]
        self._setAttributes()

    def getName(self) -> str:
        return self._name

    def getPriority(self) -> int | None:
        return self._info.get("Priority")

    def getTotalJobs(self) -> int:
        return self._info.get("total_jobs") or 0

    def getRunningJobs(self) -> int:
        return self._job_numbers.get("Running") or 0

    def getQueuedJobs(self) -> int:
        return self._job_numbers.get("Queued") or 0

    def getOtherJobs(self) -> int:
        return (
            (self._job_numbers.get("Transit") or 0)
            + (self._job_numbers.get("Held") or 0)
            + (self._job_numbers.get("Waiting") or 0)
            + (self._job_numbers.get("Exiting") or 0)
            + (self._job_numbers.get("Begun") or 0)
        )

    def getMaxWalltime(self) -> timedelta | None:
        if raw_time := self._info.get("resources_max.walltime"):
            return hhmmss_to_duration(raw_time)

        return None

    def getComment(self) -> str:
        if not (raw_comment := self._info.get("comment")):
            return ""

        return raw_comment.split("|", 1)[0]

    def isAvailableToUser(self, user: str) -> bool:
        # queues that are not enabled or not started are unavailable to all users
        if self._info.get("enabled") != "True" or self._info.get("started") != "True":
            return False

        # check acl users
        if self._info.get("acl_user_enable") == "True":
            acl_users = self._acl_users
            if user not in acl_users:
                return False

        # check acl groups
        if self._info.get("acl_group_enable") == "True":
            expected_acl_groups = self._acl_groups
            users_acl_groups = ACLData.getGroupsOrInit(user)
            if not any(item in expected_acl_groups for item in users_acl_groups):
                return False

        # check acl hosts
        if (host := self._info.get("acl_host_enable")) == "True":
            acl_hosts = self._acl_hosts
            host = ACLData.getHostOrInit()
            if host not in acl_hosts:
                return False

        return True

    def getDestinations(self) -> list[str]:
        if raw_destinations := self._info.get("route_destinations"):
            return raw_destinations.split(",")

        return []

    def fromRouteOnly(self) -> bool:
        return self._info.get("from_route_only") == "True"

    def toYaml(self) -> str:
        # we need to add queue name to the start of the dictionary
        to_dump = {"Queue": self._name} | self._info
        return yaml.dump(
            to_dump, default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    def _setAttributes(self) -> None:
        """
        Initialize derived queue attributes to avoid redundant parsing.
        """
        self._setJobNumbers()
        self._acl_users = self._info.get("acl_users", "").split(",")
        self._acl_groups = self._info.get("acl_groups", "").split(",")
        self._acl_hosts = self._info.get("acl_hosts", "").split(",")

    @classmethod
    def fromDict(cls, name: str, info: dict[str, str]) -> Self:
        """
        Construct a new instance of PBSQueue from a queue name and a dictionary of queue information.


        Args:
            name (str): The unique name of the queue.
            info (dict[str, str]): A dictionary containing PBS queue metadata as key-value pairs.

        Returns:
            Self: A new instance of PBSQueue.

        Note:
            This method does not perform any validation or processing of the provided dictionary.
        """
        job_info = cls.__new__(cls)
        job_info._name = name
        job_info._info = info
        job_info._setAttributes()

        return job_info

    @staticmethod
    def _parsePBSDumpToDictionary(text: str) -> dict[str, str]:
        """
        Parse a PBS queue info dump into a dictionary.

        Returns:
            dict[str, str]: Dictionary mapping keys to values.
        """
        result: dict[str, str] = {}

        for raw_line in text.splitlines():
            line = raw_line.rstrip()

            if " = " not in line:
                continue

            key, value = line.split(" = ", 1)
            result[key.strip()] = value.strip()

        return result

    @staticmethod
    def _parseMultiPBSDumpToDictionaries(text: str) -> list[tuple[dict[str, str], str]]:
        """
        Parse a PBS queue dump containing metadata for multiple queues into structured dictionaries.

        Args:
            text (str): The raw PBS queue dump containing information about one or more queues.

        Returns:
            list[tuple[dict[str, str], str]]: A list of tuples, each containing:
                - dict[str, str]: Parsed queue metadata for a single queue.
                - str: The queue name extracted from queue information.

        Raises:
            QQError: If the queue name cannot be extracted.
        """
        if text.strip() == "":
            return []

        data = []

        job_id_pattern = re.compile(r"^\s*Queue:\s*(.*)$")
        for chunk in text.rstrip().split("\n\n"):
            try:
                first_line = chunk.splitlines()[0]
                match = job_id_pattern.match(first_line)
                if not match:
                    raise

                job_id = match.group(1)
            except Exception as e:
                raise QQError(
                    f"Invalid PBS dump format. Could not extract queue name from:\n{chunk}"
                ) from e

            data.append((PBSQueue._parsePBSDumpToDictionary(chunk), job_id))  # ty: ignore[possibly-unbound-attribute]

        logger.debug(f"Detected and parsed metadata for {len(data)} PBS queues.")
        return data

    def _setJobNumbers(self) -> None:
        """
        Parse and store job counts by state from the 'state_count' field.

        If parsing fails or the field is missing, `_job_numbers` is set to an empty dictionary.
        """
        if not (state_count := self._info.get("state_count")):
            self._job_numbers = {}

        try:
            self._job_numbers = {
                k: int(v) for k, v in (p.split(":") for p in state_count.split())
            }
        except Exception as e:
            logger.warning(f"Could not get job counts for queue '{self._name}': {e}.")
            self._job_numbers = {}
