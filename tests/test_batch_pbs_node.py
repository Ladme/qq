# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from unittest.mock import MagicMock, patch

import pytest
import yaml

from qq_lib.batch.pbs.node import PBSNode, QueuesAvailability
from qq_lib.core.error import QQError
from qq_lib.properties.size import Size


def test_queues_availability_get_or_init_returns_cached_value():
    QueuesAvailability._queues = {"gpu": {"user1": True}}
    with patch("qq_lib.batch.pbs.node.PBSQueue") as mock_pbsqueue:
        result = QueuesAvailability.getOrInit("gpu", "user1")
    mock_pbsqueue.assert_not_called()
    assert result is True


@patch("qq_lib.batch.pbs.node.PBSQueue")
def test_queues_availability_get_or_init_queries_and_caches_value(mock_pbsqueue):
    QueuesAvailability._queues = {}
    mock_instance = MagicMock()
    mock_instance.isAvailableToUser.return_value = True
    mock_pbsqueue.return_value = mock_instance
    result = QueuesAvailability.getOrInit("cpu", "user2")
    mock_pbsqueue.assert_called_once_with("cpu")
    mock_instance.isAvailableToUser.assert_called_once_with("user2")
    assert result is True
    assert QueuesAvailability._queues == {"cpu": {"user2": True}}


@patch("qq_lib.batch.pbs.node.PBSQueue")
def test_queues_availability_get_or_init_adds_to_existing_queue_entry(mock_pbsqueue):
    QueuesAvailability._queues = {"gpu": {"user1": True}}
    mock_instance = MagicMock()
    mock_instance.isAvailableToUser.return_value = False
    mock_pbsqueue.return_value = mock_instance
    result = QueuesAvailability.getOrInit("gpu", "user2")
    mock_pbsqueue.assert_called_once_with("gpu")
    mock_instance.isAvailableToUser.assert_called_once_with("user2")
    assert result is False
    assert QueuesAvailability._queues["gpu"]["user2"] is False
    assert QueuesAvailability._queues["gpu"]["user1"] is True


@patch.object(PBSNode, "update")
def test_pbs_node_init_calls_update(mock_update):
    node = PBSNode("node1")
    mock_update.assert_called_once()
    assert node._name == "node1"
    assert isinstance(node._info, dict)
    assert node._info == {}


@patch("qq_lib.batch.pbs.node.subprocess.run")
@patch("qq_lib.batch.pbs.node.parse_pbs_dump_to_dictionary")
def test_pbs_node_update_parses_successfully(mock_parse, mock_run):
    node = PBSNode.__new__(PBSNode)
    node._name = "node1"
    mock_run.return_value = MagicMock(returncode=0, stdout="mock_output")
    mock_parse.return_value = {"state": "free", "ntype": "PBS"}
    node.update()
    mock_run.assert_called_once_with(
        ["bash"],
        input="pbsnodes -v node1",
        text=True,
        check=False,
        capture_output=True,
        errors="replace",
    )
    mock_parse.assert_called_once_with("mock_output")
    assert node._info == {"state": "free", "ntype": "PBS"}


@patch("qq_lib.batch.pbs.node.subprocess.run")
def test_pbs_node_update_raises_on_nonzero_return(mock_run):
    node = PBSNode.__new__(PBSNode)
    node._name = "nodeX"
    mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
    with pytest.raises(QQError, match="Node 'nodeX' does not exist."):
        node.update()
    mock_run.assert_called_once()


@patch("qq_lib.batch.pbs.node.subprocess.run")
@patch("qq_lib.batch.pbs.node.parse_pbs_dump_to_dictionary")
def test_pbs_node_update_sets_info_even_if_parse_returns_empty(mock_parse, mock_run):
    node = PBSNode.__new__(PBSNode)
    node._name = "node_empty"
    mock_run.return_value = MagicMock(returncode=0, stdout="")
    mock_parse.return_value = {}
    node.update()
    assert node._info == {}


def test_pbs_node_get_int_resource_returns_valid_int():
    node = PBSNode.__new__(PBSNode)
    node._info = {"resources_available.ncpus": "32"}
    result = node._getIntResource("resources_available.ncpus")
    assert result == 32


def test_pbs_node_get_int_resource_returns_zero_when_missing():
    node = PBSNode.__new__(PBSNode)
    node._info = {}
    result = node._getIntResource("resources_available.ncpus")
    assert result == 0


@patch("qq_lib.batch.pbs.node.logger.debug")
def test_pbs_node_get_int_resource_returns_zero_on_invalid_int(mock_logger_debug):
    node = PBSNode.__new__(PBSNode)
    node._info = {"resources_available.ncpus": "not_an_int"}
    result = node._getIntResource("resources_available.ncpus")
    assert result == 0
    mock_logger_debug.assert_called_once()


def test_pbs_node_get_free_int_resource_returns_correct_difference():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.ncpus": "32",
        "resources_assigned.ncpus": "8",
    }
    result = node._getFreeIntResource("ncpus")
    assert result == 24


def test_pbs_node_get_free_int_resource_returns_zero_when_negative():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.ncpus": "4",
        "resources_assigned.ncpus": "8",
    }
    result = node._getFreeIntResource("ncpus")
    assert result == 0


def test_pbs_node_get_free_int_resource_returns_zero_when_values_missing():
    node = PBSNode.__new__(PBSNode)
    node._info = {}
    result = node._getFreeIntResource("ngpus")
    assert result == 0


def test_pbs_node_get_size_resource_returns_valid_size():
    node = PBSNode.__new__(PBSNode)
    node._info = {"resources_available.mem": "8gb"}
    result = node._getSizeResource("resources_available.mem")
    assert isinstance(result, Size)
    assert result.value == 8388608


def test_pbs_node_get_size_resource_returns_zero_when_missing():
    node = PBSNode.__new__(PBSNode)
    node._info = {}
    result = node._getSizeResource("resources_available.mem")
    assert isinstance(result, Size)
    assert result.value == 0


def test_pbs_node_get_size_resource_returns_zero_when_invalid_value():
    node = PBSNode.__new__(PBSNode)
    node._info = {"resources_available.mem": "invalid_value"}
    result = node._getSizeResource("resources_available.mem")
    assert isinstance(result, Size)
    assert result.value == 0


def test_pbs_node_get_free_size_resource_returns_correct_difference():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.mem": "16gb",
        "resources_assigned.mem": "4gb",
    }
    result = node._getFreeSizeResource("mem")
    assert isinstance(result, Size)
    assert result.value == 12582912


def test_pbs_node_get_free_size_resource_returns_zero_when_negative():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.mem": "2gb",
        "resources_assigned.mem": "4gb",
    }
    result = node._getFreeSizeResource("mem")
    assert isinstance(result, Size)
    assert result.value == 0


def test_pbs_node_get_free_size_resource_returns_zero_when_missing():
    node = PBSNode.__new__(PBSNode)
    node._info = {}
    result = node._getFreeSizeResource("mem")
    assert isinstance(result, Size)
    assert result.value == 0


def test_pbs_node_get_ncpus_returns_int():
    node = PBSNode.__new__(PBSNode)
    node._info = {"resources_available.ncpus": "32"}
    assert node.getNCPUs() == 32


def test_pbs_node_get_nfree_cpus_returns_difference():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.ncpus": "32",
        "resources_assigned.ncpus": "12",
    }
    assert node.getNFreeCPUs() == 20


def test_pbs_node_get_ngpus_returns_int():
    node = PBSNode.__new__(PBSNode)
    node._info = {"resources_available.ngpus": "4"}
    assert node.getNGPUs() == 4


def test_pbs_node_get_nfree_gpus_returns_difference():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.ngpus": "4",
        "resources_assigned.ngpus": "1",
    }
    assert node.getNFreeGPUs() == 3


def test_pbs_node_get_cpu_memory_returns_size():
    node = PBSNode.__new__(PBSNode)
    node._info = {"resources_available.mem": "64gb"}
    result = node.getCPUMemory()
    assert isinstance(result, Size)
    assert result.value == 67108864


def test_pbs_node_get_free_cpu_memory_returns_difference():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.mem": "64gb",
        "resources_assigned.mem": "16gb",
    }
    result = node.getFreeCPUMemory()
    assert isinstance(result, Size)
    assert result.value == 50331648


def test_pbs_node_get_gpu_memory_returns_size():
    node = PBSNode.__new__(PBSNode)
    node._info = {"resources_available.gpu_mem": "24gb"}
    result = node.getGPUMemory()
    assert isinstance(result, Size)
    assert result.value == 25165824


def test_pbs_node_get_free_gpu_memory_returns_difference():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.gpu_mem": "24gb",
        "resources_assigned.gpu_mem": "8gb",
    }
    result = node.getFreeGPUMemory()
    assert isinstance(result, Size)
    assert result.value == 16777216


def test_pbs_node_get_local_scratch_returns_size():
    node = PBSNode.__new__(PBSNode)
    node._info = {"resources_available.scratch_local": "100gb"}
    result = node.getLocalScratch()
    assert isinstance(result, Size)
    assert result.value == 104857600


def test_pbs_node_get_free_local_scratch_returns_difference():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.scratch_local": "100gb",
        "resources_assigned.scratch_local": "40gb",
    }
    result = node.getFreeLocalScratch()
    assert isinstance(result, Size)
    assert result.value == 62914560


def test_pbs_node_get_ssd_scratch_returns_size():
    node = PBSNode.__new__(PBSNode)
    node._info = {"resources_available.scratch_ssd": "50gb"}
    result = node.getSSDScratch()
    assert isinstance(result, Size)
    assert result.value == 52428800


def test_pbs_node_get_free_ssd_scratch_returns_difference():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.scratch_ssd": "50gb",
        "resources_assigned.scratch_ssd": "10gb",
    }
    result = node.getFreeSSDScratch()
    assert isinstance(result, Size)
    assert result.value == 41943040


def test_pbs_node_get_shared_scratch_returns_size():
    node = PBSNode.__new__(PBSNode)
    node._info = {"resources_available.scratch_shared": "200gb"}
    result = node.getSharedScratch()
    assert isinstance(result, Size)
    assert result.value == 209715200


def test_pbs_node_get_free_shared_scratch_returns_difference():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.scratch_shared": "200gb",
        "resources_assigned.scratch_shared": "50gb",
    }
    result = node.getFreeSharedScratch()
    assert isinstance(result, Size)
    assert result.value == 157286400


def test_pbs_node_get_properties_returns_matching_true_keys():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.cl_cluster": "True",
        "resources_available.scratch_shm": "False",
        "resources_available.singularity": "True",
        "resv_enable": "True",
    }
    result = node.getProperties()
    assert isinstance(result, list)
    assert set(result) == {"cl_cluster", "singularity"}


def test_pbs_node_get_properties_returns_empty_when_no_true_values():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "resources_available.scratch_shm": "False",
        "resources_available.singularity": "False",
    }
    result = node.getProperties()
    assert result == []


def test_pbs_node_get_properties_returns_empty_when_no_matching_keys():
    node = PBSNode.__new__(PBSNode)
    node._info = {
        "scratch_shm": "True",
        "resv_enable": "True",
    }
    result = node.getProperties()
    assert result == []


def test_pbs_node_from_dict_creates_instance_with_correct_attributes():
    info = {"resources_available.ncpus": "32", "state": "free"}
    node = PBSNode.fromDict("nodeA", info)
    assert isinstance(node, PBSNode)
    assert node._name == "nodeA"
    assert node._info == info


def test_pbs_node_from_dict_allows_empty_info_dict():
    node = PBSNode.fromDict("nodeB", {})
    assert isinstance(node, PBSNode)
    assert node._name == "nodeB"
    assert node._info == {}


def test_pbs_node_is_available_to_user_returns_false_when_state_missing():
    node = PBSNode.__new__(PBSNode)
    node._name = "node1"
    node._info = {}
    result = node.isAvailableToUser("user1")
    assert result is False


@pytest.mark.parametrize("state", ["down", "unknown", "unresolvable", "resv-exclusive"])
def test_pbs_node_is_available_to_user_returns_false_for_disabled_states(state):
    node = PBSNode.__new__(PBSNode)
    node._name = "node2"
    node._info = {"state": state}
    result = node.isAvailableToUser("user2")
    assert result is False


@patch("qq_lib.batch.pbs.node.QueuesAvailability.getOrInit", return_value=True)
def test_pbs_node_is_available_to_user_delegates_to_queues_availability(mock_getorinit):
    node = PBSNode.__new__(PBSNode)
    node._name = "node3"
    node._info = {"state": "free", "queue": "gpu"}
    result = node.isAvailableToUser("user3")
    mock_getorinit.assert_called_once_with("gpu", "user3")
    assert result is True


def test_pbs_node_is_available_to_user_returns_true_when_no_queue_and_enabled_state():
    node = PBSNode.__new__(PBSNode)
    node._name = "node4"
    node._info = {"state": "free"}
    result = node.isAvailableToUser("user4")
    assert result is True


def test_pbs_node_get_name_returns_correct_name():
    node = PBSNode.__new__(PBSNode)
    node._name = "node1"
    assert node.getName() == "node1"


def test_pbs_node_to_yaml():
    node = PBSNode.__new__(PBSNode)
    node._name = "zeroc1"

    node._info = {
        "Mom": "zeroc1.ceitec.muni.cz",
        "state": "free",
        "pcpus": "128",
        "Priority": "80",
        "resources_available.arch": "linux",
        "resources_available.cpu_vendor": "amd",
        "resources_available.os": "debian12",
        "resources_available.mem": "257761mb",
        "resources_available.ncpus": "64",
        "resources_available.ngpus": "0",
        "resources_available.scratch_local": "920805536kb",
        "resources_available.scratch_ssd": "920805536kb",
        "resources_available.scratch_shared": "0kb",
        "resources_available.singularity": "True",
        "resources_assigned.mem": "0kb",
        "resources_assigned.ncpus": "0",
        "resources_assigned.vmem": "0kb",
        "comment_aux": "HEALTH-CHECK: OK",
        "resv_enable": "True",
        "sharing": "default_shared",
        "license": "l",
        "last_state_change_time": "Wed Oct 22 16:52:29 2025",
        "last_used_time": "Mon Oct 20 18:30:34 2025",
    }

    result = node.toYaml()
    expected_dict = {"Node": "zeroc1"} | node._info
    parsed_result = yaml.load(result, Loader=yaml.SafeLoader)

    assert isinstance(result, str)
    assert isinstance(parsed_result, dict)
    assert parsed_result == expected_dict
