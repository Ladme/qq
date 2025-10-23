# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import socket
import subprocess
from dataclasses import fields
from datetime import timedelta
from typing import Self

import yaml

from qq_lib.batch.interface.queue import BatchQueueInterface
from qq_lib.batch.pbs.common import parsePBSDumpToDictionary
from qq_lib.core.common import hhmmss_to_duration
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.resources import QQResources

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
            errors="replace",
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
    Implementation of BatchQueueInterface for PBS.
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
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(f"Queue '{self._name}' does not exist.")

        self._info = parsePBSDumpToDictionary(result.stdout)  # ty: ignore[possibly-unbound-attribute]
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

    def getDefaultResources(self) -> dict[str, str]:
        default_resources = {}

        for key, value in self._info.items():
            if "resources_default" in key:
                resource = key.split(".")[-1]
                default_resources[resource.strip()] = value.strip()

        # only return resources that are part of QQResources
        field_names = {f.name for f in fields(QQResources)}
        return {k: v for k, v in default_resources.items() if k in field_names}

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
        queue = cls.__new__(cls)
        queue._name = name
        queue._info = info
        queue._setAttributes()

        return queue

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
