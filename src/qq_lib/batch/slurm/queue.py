# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import subprocess
from datetime import timedelta
from typing import Self

import yaml

from qq_lib.batch.interface.queue import BatchQueueInterface
from qq_lib.batch.slurm.common import (
    default_resources_from_dict,
    parse_slurm_dump_to_dictionary,
)
from qq_lib.core.common import dhhmmss_to_duration, load_yaml_dumper
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.resources import Resources

logger = get_logger(__name__)

Dumper: type[yaml.Dumper] = load_yaml_dumper()


class UserGroups:
    """
    Utility class for caching and retrieving user groups and QOS.
    """

    _groups: dict[str, list[str]] = {}
    _qos: dict[str, str] = {}

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
        if groups := UserGroups._groups.get(user):
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
            UserGroups._groups[user] = []
            return []

        groups = result.stdout.split()
        UserGroups._groups[user] = groups
        logger.debug(f"Initialized groups for user '{user}': {groups}.")
        return groups

    @staticmethod
    def getQOSOrInit(user: str) -> str:
        if qos := UserGroups._qos.get(user):
            return qos

        result = subprocess.run(
            ["bash"],
            input=f"sacctmgr show user {user} format=qos -n -P",
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0 or result.stdout.strip() == "":
            # set the default QOS
            UserGroups._qos[user] = "normal"
            return "normal"

        # if multiple QOS are available, use the first one
        qos_list = [q.strip() for q in result.stdout.strip().split(",") if q.strip()]
        if qos_list:
            return qos_list[0]

        return "normal"


class SlurmQueue(BatchQueueInterface):
    """
    Implementation of BatchQueueInterface for Slurm.
    Stores metadata for a single Slurm queue.
    """

    def __init__(self, name: str):
        self._name = name
        self._info: dict[str, str] = {}

        self.update()

    def update(self) -> None:
        # get queue info from Slurm
        command = f"scontrol show partition {self._name} -o"
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
            raise QQError(f"Queue '{self._name}' does not exist.")

        self._info = parse_slurm_dump_to_dictionary(result.stdout)
        self._setJobNumbers()

    def getName(self) -> str:
        return self._name

    def getPriority(self) -> str | None:
        if not (tier := self._info.get("PriorityTier")):
            return None

        if not (job_factor := self._info.get("PriorityJobFactor")):
            return None

        return f"T{tier} ({job_factor})"

    def getTotalJobs(self) -> int | None:
        return self._running_jobs + self._queued_jobs + self._other_jobs

    def getRunningJobs(self) -> int | None:
        return self._running_jobs

    def getQueuedJobs(self) -> int | None:
        return self._queued_jobs

    def getOtherJobs(self) -> int | None:
        return self._other_jobs

    def getMaxWalltime(self) -> timedelta | None:
        if raw := self._info.get("MaxTime"):
            return dhhmmss_to_duration(raw)

        return None

    def getMaxNNodes(self) -> int | None:
        if not (raw := self._info.get("MaxNodes")):
            return None

        try:
            return int(raw)
        except ValueError as e:
            logger.debug(f"Could not parse the 'MaxNodes' property as integer: {e}.")
            return None

    def getComment(self) -> str | None:
        return None

    def isAvailableToUser(self, user: str) -> bool:
        # check the queue's state
        state = self._info.get("State", "DOWN")
        if state not in ["UP", "DRAIN"]:
            return False

        def parse_list(value):
            if not value or value == "(null)" or value == "ALL":
                return None
            return [item.strip() for item in value.split(",")]

        # check allowed accounts
        allow_accounts = parse_list(self._info.get("AllowAccounts", "ALL"))
        if allow_accounts and user not in allow_accounts:
            return False

        # check denied accounts
        deny_accounts = parse_list(self._info.get("DenyAccounts", "(null)"))
        if deny_accounts and user in deny_accounts:
            return False

        # check allowed groups
        user_groups = UserGroups.getGroupsOrInit(user)
        allow_groups = parse_list(self._info.get("AllowGroups", "ALL"))
        if allow_groups and not any(group in allow_groups for group in user_groups):
            return False

        # check denied groups
        deny_groups = parse_list(self._info.get("DenyGroups", "(null)"))
        if deny_groups and any(group in deny_groups for group in user_groups):
            return False

        # check allowed QOS
        user_qos = UserGroups.getQOSOrInit(user)
        allow_qos = parse_list(self._info.get("AllowQos", "ALL"))
        if allow_qos and user_qos not in allow_qos:
            return False

        # check denies QOS
        deny_qos = parse_list(self._info.get("DenyQos", "(null)"))
        return not (deny_qos and user_qos in deny_qos)

    def getDestinations(self) -> list[str]:
        # no destinations
        return []

    def fromRouteOnly(self) -> bool:
        return False

    def toYaml(self) -> str:
        return yaml.dump(
            self._info, default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    def getDefaultResources(self) -> Resources:
        return default_resources_from_dict(self._info)

    @classmethod
    def fromDict(cls, name: str, info: dict[str, str]) -> Self:
        """
        Construct a new instance of SlurmQueue from a queue name and a dictionary of queue information.


        Args:
            name (str): The unique name of the queue.
            info (dict[str, str]): A dictionary containing Slurm queue metadata as key-value pairs.

        Returns:
            Self: A new instance of SlurmQueue.

        Note:
            This method does not perform any validation or processing of the provided dictionary.
        """
        queue = cls.__new__(cls)
        queue._name = name
        queue._info = info
        queue._setJobNumbers()

        return queue

    def _setJobNumbers(self) -> None:
        """
        Get and set the numbers of jobs in this queue.
        """
        self._running_jobs = 0
        self._queued_jobs = 0
        self._other_jobs = 0

        # get the numbers of jobs in the queue by individual states
        command = f'squeue -p {self._name} -h -o "%T" | uniq -c'
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
            raise QQError(
                f"Could not get job numbers for queue '{self._name}': {result.stderr.strip()}."
            )

        for line in result.stdout.splitlines():
            if not line.strip():
                continue

            try:
                count_str, job_type = line.split()
                count = int(count_str)
            except ValueError as e:
                logger.warning(
                    f"Could not parse line '{line}' when obtaining job numbers for queue '{self._name}': {e}."
                )
                continue

            match job_type:
                case "RUNNING":
                    self._running_jobs += count
                case "PENDING":
                    self._queued_jobs += count
                case "SUSPENDED" | "PREEMPTED":
                    self._other_jobs += count
                # ignore other jobs
