# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
import yaml

from qq_lib.batch.pbs.queue import ACLData, PBSQueue
from qq_lib.core.error import QQError


def test_acldata_get_groups_or_init_cached():
    ACLData._groups = {"user": ["staff", "users"]}
    result = ACLData.getGroupsOrInit("user")
    assert result == ["staff", "users"]


def test_acldata_get_groups_or_init_success():
    ACLData._groups.clear()
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "dev admin"
    with (
        patch(
            "qq_lib.batch.pbs.queue.subprocess.run", return_value=mock_result
        ) as run_mock,
        patch("qq_lib.batch.pbs.queue.logger"),
    ):
        result = ACLData.getGroupsOrInit("user")
    run_mock.assert_called_once_with(
        ["bash"],
        input="id -nG user",
        text=True,
        check=False,
        capture_output=True,
        errors="replace",
    )
    assert result == ["dev", "admin"]
    assert ACLData._groups["user"] == ["dev", "admin"]


def test_acldata_get_groups_or_init_failure():
    ACLData._groups.clear()
    mock_result = MagicMock()
    mock_result.returncode = 1
    with patch("qq_lib.batch.pbs.queue.subprocess.run", return_value=mock_result):
        result = ACLData.getGroupsOrInit("user")
    assert result == []
    assert ACLData._groups["user"] == []


def test_acldata_get_host_or_init_cached():
    ACLData._host = "cached.host"
    assert ACLData.getHostOrInit() == "cached.host"


def test_acldata_get_host_or_init_initializes_and_caches():
    ACLData._host = None
    with (
        patch(
            "qq_lib.batch.pbs.queue.socket.gethostname", return_value="new.host.org"
        ) as mock_get,
        patch("qq_lib.batch.pbs.queue.logger"),
    ):
        host = ACLData.getHostOrInit()
    mock_get.assert_called_once()
    assert host == "new.host.org"
    assert ACLData._host == "new.host.org"


def test_pbsqueue_init():
    with patch.object(PBSQueue, "update") as mock_update:
        queue = PBSQueue("main")
    assert queue._name == "main"
    assert isinstance(queue._info, dict)
    mock_update.assert_called_once()


def test_pbsqueue_update_success():
    queue = PBSQueue.__new__(PBSQueue)
    queue._name = "main"
    queue._info = {}

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "queue_data"

    with (
        patch(
            "qq_lib.batch.pbs.queue.subprocess.run", return_value=mock_result
        ) as run_mock,
        patch(
            "qq_lib.batch.pbs.queue.parse_pbs_dump_to_dictionary",
            return_value={"k": "v"},
        ) as parse_mock,
        patch.object(queue, "_setAttributes") as set_attrs_mock,
    ):
        queue.update()

    run_mock.assert_called_once_with(
        ["bash"],
        input="qstat -Qfw main",
        text=True,
        check=False,
        capture_output=True,
        errors="replace",
    )
    parse_mock.assert_called_once_with("queue_data")
    set_attrs_mock.assert_called_once()
    assert queue._info == {"k": "v"}


def test_pbsqueue_update_failure():
    queue = PBSQueue.__new__(PBSQueue)
    queue._name = "nonexistent"

    mock_result = MagicMock()
    mock_result.returncode = 1

    with (
        patch("qq_lib.batch.pbs.queue.subprocess.run", return_value=mock_result),
        pytest.raises(QQError, match="Queue 'nonexistent' does not exist."),
    ):
        queue.update()


def test_pbsqueue_get_name():
    queue = PBSQueue.__new__(PBSQueue)
    queue._name = "main"
    assert queue.getName() == "main"


def test_pbsqueue_get_priority_returns_value():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {"Priority": 5}
    assert queue.getPriority() == 5


def test_pbsqueue_get_priority_returns_none():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {}
    assert queue.getPriority() is None


def test_pbsqueue_get_total_jobs_with_value():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {"total_jobs": 10}
    assert queue.getTotalJobs() == 10


def test_pbsqueue_get_total_jobs_default_zero():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {}
    assert queue.getTotalJobs() == 0


def test_pbsqueue_get_running_jobs_with_value():
    queue = PBSQueue.__new__(PBSQueue)
    queue._job_numbers = {"Running": 4}
    assert queue.getRunningJobs() == 4


def test_pbsqueue_get_running_jobs_default_zero():
    queue = PBSQueue.__new__(PBSQueue)
    queue._job_numbers = {}
    assert queue.getRunningJobs() == 0


def test_pbsqueue_get_queued_jobs_with_value():
    queue = PBSQueue.__new__(PBSQueue)
    queue._job_numbers = {"Queued": 7}
    assert queue.getQueuedJobs() == 7


def test_pbsqueue_get_queued_jobs_default_zero():
    queue = PBSQueue.__new__(PBSQueue)
    queue._job_numbers = {}
    assert queue.getQueuedJobs() == 0


def test_pbsqueue_get_other_jobs_sum_all_states():
    queue = PBSQueue.__new__(PBSQueue)
    queue._job_numbers = {
        "Transit": 1,
        "Held": 2,
        "Waiting": 3,
        "Exiting": 4,
        "Begun": 5,
    }
    assert queue.getOtherJobs() == 15


def test_pbsqueue_get_other_jobs_default_zero():
    queue = PBSQueue.__new__(PBSQueue)
    queue._job_numbers = {}
    assert queue.getOtherJobs() == 0


def test_pbsqueue_get_max_walltime_returns_timedelta():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {"resources_max.walltime": "25:30:00"}

    assert queue.getMaxWalltime() == timedelta(days=1, hours=1, minutes=30)


def test_pbsqueue_get_max_walltime_none():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {}
    assert queue.getMaxWalltime() is None


def test_pbsqueue_get_comment_with_value():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {"comment": "Default queue|details"}
    assert queue.getComment() == "Default queue"


def test_pbsqueue_get_comment_empty():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {}
    assert queue.getComment() == ""


def test_pbsqueue_get_destinations_with_values():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {"route_destinations": "node1,node2,node3"}
    assert queue.getDestinations() == ["node1", "node2", "node3"]


def test_pbsqueue_get_destinations_empty():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {}
    assert queue.getDestinations() == []


def test_pbsqueue_from_route_only_true():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {"from_route_only": "True"}
    assert queue.fromRouteOnly() is True


def test_pbsqueue_from_route_only_false():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {"from_route_only": "False"}
    assert queue.fromRouteOnly() is False


def test_pbsqueue_from_route_only_missing_key():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {}
    assert queue.fromRouteOnly() is False


def test_pbsqueue_set_attributes_populates_acl_and_calls_set_job_numbers():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {
        "acl_users": "user1,user2",
        "acl_groups": "group1,group2",
        "acl_hosts": "host1,host2",
    }

    with patch.object(queue, "_setJobNumbers") as mock_set_job_numbers:
        queue._setAttributes()

    mock_set_job_numbers.assert_called_once()
    assert queue._acl_users == ["user1", "user2"]
    assert queue._acl_groups == ["group1", "group2"]
    assert queue._acl_hosts == ["host1", "host2"]


def test_pbsqueue_set_attributes_handles_missing_acls():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {}

    with patch.object(queue, "_setJobNumbers") as mock_set_job_numbers:
        queue._setAttributes()

    mock_set_job_numbers.assert_called_once()
    assert queue._acl_users == [""]
    assert queue._acl_groups == [""]
    assert queue._acl_hosts == [""]


def test_pbsqueue_set_job_numbers_parses_valid_state_count():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {"state_count": "Running:5 Queued:3 Held:1"}
    queue._name = "main"

    queue._setJobNumbers()

    assert queue._job_numbers == {"Running": 5, "Queued": 3, "Held": 1}


def test_pbsqueue_set_job_numbers_handles_missing_state_count():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {}
    queue._name = "main"

    queue._setJobNumbers()

    assert queue._job_numbers == {}


def test_pbsqueue_set_job_numbers_handles_invalid_format():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {"state_count": "InvalidFormat"}
    queue._name = "main"

    queue._setJobNumbers()
    assert queue._job_numbers == {}


def test_pbsqueue_is_available_to_user_disabled_queue():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {"enabled": "False", "started": "True"}
    assert queue.isAvailableToUser("user") is False


def test_pbsqueue_is_available_to_user_not_started_queue():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {"enabled": "True", "started": "False"}
    assert queue.isAvailableToUser("user") is False


def test_pbsqueue_is_available_to_user_acl_user_not_in_list():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {
        "enabled": "True",
        "started": "True",
        "acl_user_enable": "True",
    }
    queue._acl_users = ["otheruser"]
    assert queue.isAvailableToUser("user") is False


def test_pbsqueue_is_available_to_user_acl_group_not_in_group():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {
        "enabled": "True",
        "started": "True",
        "acl_group_enable": "True",
    }
    queue._acl_groups = ["allowed_group"]

    with patch.object(ACLData, "getGroupsOrInit", return_value=["other_group"]):
        assert queue.isAvailableToUser("user") is False


def test_pbsqueue_is_available_to_user_acl_host_not_in_list():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {
        "enabled": "True",
        "started": "True",
        "acl_host_enable": "True",
    }
    queue._acl_hosts = ["host1"]

    with patch.object(ACLData, "getHostOrInit", return_value="otherhost"):
        assert queue.isAvailableToUser("user") is False


def test_pbsqueue_is_available_to_user_all_acls_pass():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {
        "enabled": "True",
        "started": "True",
        "acl_user_enable": "True",
        "acl_group_enable": "True",
        "acl_host_enable": "True",
    }
    queue._acl_users = ["user"]
    queue._acl_groups = ["allowed_group"]
    queue._acl_hosts = ["host1"]

    with (
        patch.object(ACLData, "getGroupsOrInit", return_value=["allowed_group"]),
        patch.object(ACLData, "getHostOrInit", return_value="host1"),
    ):
        assert queue.isAvailableToUser("user") is True


def test_pbsqueue_is_available_to_user_acl_user_passes_but_group_fails():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {
        "enabled": "True",
        "started": "True",
        "acl_user_enable": "True",
        "acl_group_enable": "True",
    }
    queue._acl_users = ["user"]
    queue._acl_groups = ["allowed_group"]

    with patch.object(ACLData, "getGroupsOrInit", return_value=["other_group"]):
        assert queue.isAvailableToUser("user") is False


def test_pbsqueue_is_available_to_user_user_and_group_pass_but_host_fails():
    queue = PBSQueue.__new__(PBSQueue)
    queue._info = {
        "enabled": "True",
        "started": "True",
        "acl_user_enable": "True",
        "acl_group_enable": "True",
        "acl_host_enable": "True",
    }
    queue._acl_users = ["user"]
    queue._acl_groups = ["group1"]
    queue._acl_hosts = ["hostA"]

    with (
        patch.object(ACLData, "getGroupsOrInit", return_value=["group1"]),
        patch.object(ACLData, "getHostOrInit", return_value="hostB"),
    ):
        assert queue.isAvailableToUser("user") is False


def test_pbsqueue_from_dict():
    name = "gpu_queue"
    info = {
        "enabled": "True",
        "started": "True",
        "Priority": "100",
        "total_jobs": "42",
    }

    with patch.object(PBSQueue, "_setAttributes") as set_attributes_mock:
        queue = PBSQueue.fromDict(name, info)

    assert queue._name == name
    assert queue._info == info
    set_attributes_mock.assert_called_once()


def test_pbsqueue_to_yaml():
    queue = PBSQueue.__new__(PBSQueue)
    queue._name = "gpu_long"

    queue._info = {
        "enabled": "True",
        "started": "True",
        "Priority": "75",
        "total_jobs": "42",
        "state_count": "Running:20 Queued:15 Held:7",
        "comment": "High priority queue|for gpu jobs",
        "route_destinations": "gpu_short,gpu_test",
        "from_route_only": "False",
        "resources_max.walltime": "48:00:00",
        "acl_users": "user1,user2",
        "acl_groups": "groupA,groupB",
        "acl_hosts": "host1,host2",
    }

    result = queue.toYaml()

    expected_dict = {"Queue": "gpu_long"} | queue._info
    parsed_result = yaml.load(result, Loader=yaml.SafeLoader)

    assert parsed_result == expected_dict


def test_pbsqueue_get_default_resources_filters_correct_fields():
    queue = PBSQueue.__new__(PBSQueue)
    queue._name = "gpu"
    queue._info = {
        "resources_default.mem": "8gb",
        "resources_default.ncpus": "4",
        "resources_default.ngpus": "1",
        "resources_default.invalid_field": "should_be_filtered",
        "resources_max.walltime": "24:00:00",
    }

    expected = {
        "mem": "8gb",
        "ncpus": "4",
        "ngpus": "1",
    }

    result = queue.getDefaultResources()

    assert result == expected


def test_pbsqueue_get_default_resources_returns_empty_when_no_defaults():
    queue = PBSQueue.__new__(PBSQueue)
    queue._name = "gpu"
    queue._info = {
        "resources_max.mem": "16gb",
        "comment": "No default resources here",
    }

    assert queue.getDefaultResources() == {}
