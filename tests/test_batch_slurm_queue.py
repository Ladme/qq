# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
import yaml

from qq_lib.batch.slurm.queue import SlurmQueue, UserGroups
from qq_lib.core.error import QQError


@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_user_groups_get_groups_or_init_returns_cached_groups(mock_run):
    # groups are cached
    user = "user"
    UserGroups._groups.clear()
    UserGroups._groups[user] = ["dev", "admin"]
    result = UserGroups.getGroupsOrInit(user)
    assert result == ["dev", "admin"]
    mock_run.assert_not_called()


@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_user_groups_get_groups_or_init_initializes_on_success(mock_run):
    user = "user"
    UserGroups._groups.clear()
    mock_run.return_value = MagicMock(returncode=0, stdout="dev qa")
    result = UserGroups.getGroupsOrInit(user)
    assert result == ["dev", "qa"]
    assert UserGroups._groups[user] == ["dev", "qa"]
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_user_groups_get_groups_or_init_returns_empty_on_failure(mock_run):
    user = "user"
    UserGroups._groups.clear()
    mock_run.return_value = MagicMock(returncode=1, stdout="")
    result = UserGroups.getGroupsOrInit(user)
    assert result == []
    assert UserGroups._groups[user] == []


@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_user_groups_get_qos_or_init_returns_cached_qos(mock_run):
    # qos is cached
    user = "user"
    UserGroups._qos.clear()
    UserGroups._qos[user] = "high"
    result = UserGroups.getQOSOrInit(user)
    assert result == "high"
    mock_run.assert_not_called()


@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_user_groups_get_qos_or_init_returns_normal_on_failure(mock_run):
    user = "user"
    UserGroups._qos.clear()
    mock_run.return_value = MagicMock(returncode=1, stdout="")
    result = UserGroups.getQOSOrInit(user)
    assert result == "normal"  # default QOS
    assert UserGroups._qos[user] == "normal"


@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_user_groups_get_qos_or_init_returns_first_qos(mock_run):
    user = "user"
    UserGroups._qos.clear()
    mock_run.return_value = MagicMock(returncode=0, stdout="premium,low")
    result = UserGroups.getQOSOrInit(user)
    assert result == "premium"


@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_user_groups_get_qos_or_init_returns_normal_on_empty_stdout(mock_run):
    user = "user"
    UserGroups._qos.clear()
    mock_run.return_value = MagicMock(returncode=0, stdout="")
    result = UserGroups.getQOSOrInit(user)
    assert result == "normal"


@patch("qq_lib.batch.slurm.queue.SlurmQueue.update")
def test_slurm_queue_init_calls_update(mock_update):
    # ensure update is called during initialization
    queue = SlurmQueue("default")
    assert queue._name == "default"
    mock_update.assert_called_once()


@patch("qq_lib.batch.slurm.queue.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_slurm_queue_update(mock_run, mock_parse):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._name = "default"
    mock_run.return_value = MagicMock(
        returncode=0, stdout="PartitionName=default State=UP"
    )
    mock_parse.return_value = {"PartitionName": "default", "State": "UP"}
    with patch.object(queue, "_setJobNumbers") as mock_set_jobs:
        queue.update()
    mock_run.assert_called_once()
    mock_parse.assert_called_once_with("PartitionName=default State=UP")
    mock_set_jobs.assert_called_once()
    assert queue._info == {"PartitionName": "default", "State": "UP"}


@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_slurm_queue_update_raises_on_failure(mock_run):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._name = "cpu"
    mock_run.return_value = MagicMock(returncode=1, stdout="")
    with pytest.raises(QQError, match="Queue 'cpu' does not exist."):
        queue.update()


def test_slurm_queue_get_name():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._name = "cpu"
    result = queue.getName()
    assert result == "cpu"


def test_slurm_queue_get_priority_returns_none_when_tier_missing():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"PriorityJobFactor": "5"}
    result = queue.getPriority()
    assert result is None


def test_slurm_queue_get_priority_returns_none_when_job_factor_missing():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"PriorityTier": "2"}
    result = queue.getPriority()
    assert result is None


def test_slurm_queue_get_priority_returns_combined_string():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"PriorityTier": "2", "PriorityJobFactor": "10"}
    result = queue.getPriority()
    assert result == "T2 (10)"


def test_slurm_queue_get_total_jobs():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._running_jobs = 5
    queue._queued_jobs = 3
    queue._other_jobs = 2
    result = queue.getTotalJobs()
    assert result == 10


def test_slurm_queue_get_running_jobs():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._running_jobs = 7
    result = queue.getRunningJobs()
    assert result == 7


def test_slurm_queue_get_queued_jobs():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._queued_jobs = 4
    result = queue.getQueuedJobs()
    assert result == 4


def test_slurm_queue_get_other_jobs():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._other_jobs = 2
    result = queue.getOtherJobs()
    assert result == 2


def test_slurm_queue_get_max_walltime_converts_valid_time():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"MaxTime": "2-12:34:56"}
    result = queue.getMaxWalltime()
    assert isinstance(result, timedelta)
    assert result == timedelta(days=2, hours=12, minutes=34, seconds=56)


def test_slurm_queue_get_max_walltime_converts_simple_time():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"MaxTime": "10:00:00"}
    result = queue.getMaxWalltime()
    assert result == timedelta(hours=10)


def test_slurm_queue_get_max_walltime_returns_none_when_missing():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {}
    result = queue.getMaxWalltime()
    assert result is None


def test_slurm_queue_get_comment_returns_empty_string():
    queue = SlurmQueue.__new__(SlurmQueue)
    result = queue.getComment()
    assert result == ""


@pytest.mark.parametrize("state", ["DOWN", "INACTIVE", "MAINT", "ALLOCATED"])
def test_slurm_queue_is_available_user_false_invalid_state(state):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"State": state}
    result = queue.isAvailableToUser("user")
    assert result is False


def test_slurm_queue_is_available_user_false_not_in_allow_accounts():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"State": "UP", "AllowAccounts": "user2,user3"}
    result = queue.isAvailableToUser("user1")
    assert result is False


def test_slurm_queue_is_available_user_false_in_deny_accounts():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"State": "UP", "DenyAccounts": "user1"}
    result = queue.isAvailableToUser("user1")
    assert result is False


@patch("qq_lib.batch.slurm.queue.UserGroups.getGroupsOrInit", return_value=["qa"])
def test_slurm_queue_is_available_user_false_not_in_allow_groups(mock_groups):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"State": "UP", "AllowGroups": "dev"}
    result = queue.isAvailableToUser("user1")
    assert result is False
    mock_groups.assert_called_once_with("user1")


@patch("qq_lib.batch.slurm.queue.UserGroups.getGroupsOrInit", return_value=["dev"])
def test_slurm_queue_is_available_user_false_in_deny_groups(mock_groups):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"State": "UP", "DenyGroups": "dev"}
    result = queue.isAvailableToUser("user1")
    assert result is False
    mock_groups.assert_called_once_with("user1")


@patch("qq_lib.batch.slurm.queue.UserGroups.getGroupsOrInit", return_value=["dev"])
@patch("qq_lib.batch.slurm.queue.UserGroups.getQOSOrInit", return_value="normal")
def test_slurm_queue_is_available_user_false_qos_not_allowed(mock_qos, mock_groups):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"State": "UP", "AllowQos": "premium,high"}
    result = queue.isAvailableToUser("user1")
    assert result is False
    mock_groups.assert_called_once_with("user1")
    mock_qos.assert_called_once_with("user1")


@patch("qq_lib.batch.slurm.queue.UserGroups.getGroupsOrInit", return_value=["dev"])
@patch("qq_lib.batch.slurm.queue.UserGroups.getQOSOrInit", return_value="normal")
def test_slurm_queue_is_available_user_false_qos_denied(mock_qos, mock_groups):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"State": "UP", "DenyQos": "normal"}
    result = queue.isAvailableToUser("user1")
    assert result is False
    mock_groups.assert_called_once_with("user1")
    mock_qos.assert_called_once_with("user1")


@patch("qq_lib.batch.slurm.queue.UserGroups.getGroupsOrInit", return_value=["dev"])
@patch("qq_lib.batch.slurm.queue.UserGroups.getQOSOrInit", return_value="normal")
def test_slurm_queue_is_available_user_true_no_restrictions(mock_qos, mock_groups):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {"State": "UP"}
    result = queue.isAvailableToUser("user1")
    assert result is True
    mock_groups.assert_called_once_with("user1")
    mock_qos.assert_called_once_with("user1")


@patch("qq_lib.batch.slurm.queue.UserGroups.getGroupsOrInit", return_value=["dev"])
@patch("qq_lib.batch.slurm.queue.UserGroups.getQOSOrInit", return_value="normal")
def test_slurm_queue_is_available_user_true_all_allows(mock_qos, mock_groups):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {
        "State": "UP",
        "AllowAccounts": "ALL",
        "AllowGroups": "ALL",
        "AllowQos": "ALL",
    }
    result = queue.isAvailableToUser("user1")
    assert result is True
    mock_groups.assert_called_once_with("user1")
    mock_qos.assert_called_once_with("user1")


@patch("qq_lib.batch.slurm.queue.UserGroups.getGroupsOrInit", return_value=["dev"])
@patch("qq_lib.batch.slurm.queue.UserGroups.getQOSOrInit", return_value="normal")
def test_slurm_queue_is_available_user_true_all_allows_null_denies(
    mock_qos, mock_groups
):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {
        "State": "UP",
        "AllowAccounts": "ALL",
        "DenyAccounts": "(null)",
        "AllowGroups": "ALL",
        "DenyGroups": "(null)",
        "AllowQos": "ALL",
        "DenyQos": "(null)",
    }
    result = queue.isAvailableToUser("user1")
    assert result is True
    mock_groups.assert_called_once_with("user1")
    mock_qos.assert_called_once_with("user1")


def test_slurm_queue_get_destinations_returns_empty_list():
    queue = SlurmQueue.__new__(SlurmQueue)
    result = queue.getDestinations()
    assert result == []


def test_slurm_queue_from_route_only_returns_false():
    queue = SlurmQueue.__new__(SlurmQueue)
    result = queue.fromRouteOnly()
    assert result is False


def test_slurm_queue_to_yaml_round_trip():
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._name = "cpu"

    queue._info = {
        "PartitionName": "cpu",
        "AllowAccounts": "ALL",
        "AllowQos": "ALL",
        "DefaultTime": "1-00:00:00",
        "MaxTime": "2-00:00:00",
        "Nodes": "node1",
        "PriorityJobFactor": "2",
        "PriorityTier": "1",
        "State": "UP",
        "TRES": "cpu=768,mem=24300000M,node=1,billing=768",
    }

    result = queue.toYaml()
    parsed = yaml.load(result, Loader=yaml.SafeLoader)

    assert parsed == queue._info


@patch(
    "qq_lib.batch.slurm.queue.default_resources_from_dict",
    return_value="mocked_resources",
)
def test_slurm_queue_get_default_resources_calls_helper(mock_default):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._info = {
        "PartitionName": "cpu",
        "DefMemPerCPU": "30800",
        "DefaultTime": "1-00:00:00",
        "MaxTime": "2-00:00:00",
        "Nodes": "node1",
        "AllowAccounts": "ALL",
        "State": "UP",
    }

    result = queue.getDefaultResources()

    assert result == "mocked_resources"
    mock_default.assert_called_once_with(
        {
            "PartitionName": "cpu",
            "DefMemPerCPU": "30800",
            "DefaultTime": "1-00:00:00",
            "MaxTime": "2-00:00:00",
            "Nodes": "node1",
            "AllowAccounts": "ALL",
            "State": "UP",
        }
    )


@patch("qq_lib.batch.slurm.queue.logger.warning")
@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_slurm_queue_set_job_numbers(mock_run, mock_warning):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._name = "cpu"
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout="5 RUNNING\n2 CANCELLED\n3 PENDING\n\n2 SUSPENDED\n5 COMPLETED\n1 PREEMPTED\n",
        stderr="",
    )

    queue._setJobNumbers()

    assert queue._running_jobs == 5
    assert queue._queued_jobs == 3
    assert queue._other_jobs == 3
    mock_warning.assert_not_called()


@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_slurm_queue_set_job_numbers_raises_on_failure(mock_run):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._name = "cpu"
    mock_run.return_value = MagicMock(returncode=1, stderr="permission denied")
    with pytest.raises(
        QQError, match="Could not get job numbers for queue 'cpu': permission denied."
    ):
        queue._setJobNumbers()


@patch("qq_lib.batch.slurm.queue.logger.warning")
@patch("qq_lib.batch.slurm.queue.subprocess.run")
def test_slurm_queue_set_job_numbers_handles_invalid_lines(mock_run, mock_warning):
    queue = SlurmQueue.__new__(SlurmQueue)
    queue._name = "cpu"
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout="5 RUNNING\nabc PENDING\n",
        stderr="",
    )

    queue._setJobNumbers()

    assert queue._queued_jobs == 0
    assert queue._running_jobs == 5
    assert queue._other_jobs == 0
    mock_warning.assert_called_once()


@patch.object(SlurmQueue, "_setJobNumbers")
def test_slurm_queue_from_dict_creates_instance(mock_set_jobs):
    name = "cpu"
    info = {"PartitionName": "cpu", "State": "UP", "MaxTime": "2-00:00:00"}

    queue = SlurmQueue.fromDict(name, info)

    assert isinstance(queue, SlurmQueue)
    assert queue._name == "cpu"
    assert queue._info == info
    mock_set_jobs.assert_called_once_with()
