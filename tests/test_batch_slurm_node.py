# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

import pytest
import yaml

from qq_lib.batch.slurm.node import SlurmNode
from qq_lib.core.error import QQError
from qq_lib.properties.size import Size


@patch("qq_lib.batch.slurm.node.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.node.subprocess.run")
def test_slurm_node_init_update_called(mock_run, mock_parser):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "output"
    mock_run.return_value = mock_result
    mock_parser.return_value = {}

    node = SlurmNode("node1")
    assert isinstance(node._info, dict)
    mock_run.assert_called_once()
    mock_parser.assert_called_once()


@patch("qq_lib.batch.slurm.node.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.node.subprocess.run")
def test_slurm_node_update_success(mock_run, mock_parser):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "node_info_output"
    mock_run.return_value = mock_result
    mock_parser.return_value = {"NodeName": "node1"}

    node = SlurmNode("node1")

    mock_run.assert_called_once_with(
        ["bash"],
        input="scontrol show node node1 -o",
        text=True,
        check=False,
        capture_output=True,
        errors="replace",
    )

    mock_parser.assert_called_once_with("node_info_output")
    assert node._info == {"NodeName": "node1"}


@patch("qq_lib.batch.slurm.node.subprocess.run")
def test_slurm_node_update_failure_raises_qqerror(mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_run.return_value = mock_result

    with pytest.raises(QQError, match="Node 'node2' does not exist."):
        SlurmNode("node2")


def test_slurm_node_get_size_resource_numeric_value_adds_m():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {"mem": "1024"}

    result = node._getSizeResource("mem")

    assert isinstance(result, Size)
    assert result.value == 1024 * 1024  # in kb


def test_slurm_node_get_size_resource_non_numeric_value():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {"mem": "1024M"}

    result = node._getSizeResource("mem")

    assert isinstance(result, Size)
    assert result.value == 1024 * 1024


def test_slurm_node_get_size_resource_returns_zero_if_missing():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {}

    result = node._getSizeResource("mem")

    assert isinstance(result, Size)
    assert result.value == 0


def test_slurm_node_get_size_resource_exception_returns_zero():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {"mem": "invalid"}

    result = node._getSizeResource("mem")

    assert isinstance(result, Size)
    assert result.value == 0


def test_slurm_node_get_int_from_tres_returns_cpu():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {"tres": "cpu=128,mem=250G,billing=128"}

    result = node._getIntFromTres("tres", "cpu")

    assert result == 128


def test_slurm_node_get_int_from_tres_returns_gpu():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {"tres": "cpu=128,mem=250G,billing=128,gres/gpu=8"}

    result = node._getIntFromTres("tres", "gpu")

    assert result == 8


def test_slurm_node_get_int_from_tres_returns_zero_if_key_missing():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {}

    result = node._getIntFromTres("tres", "gpu")

    assert result == 0


def test_slurm_node_get_int_from_tres_returns_zero_if_res_not_found():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {"tres": "cpu=128,mem=250G,billing=128"}

    result = node._getIntFromTres("tres", "gpu")

    assert result == 0


def test_slurm_node_get_int_from_tres_invalid_value_returns_zero():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {"tres": "cpu=invalid,mem=250G,billing=128"}

    result = node._getIntFromTres("tres", "cpu")

    assert result == 0


def test_slurm_node_get_int_resource_returns_integer_value():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {"CPUTot": "128"}

    result = node._getIntResource("CPUTot")
    assert result == 128


def test_slurm_node_get_int_resource_returns_zero_if_missing():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {}

    result = node._getIntResource("CPUTot")
    assert result == 0


def test_slurm_node_get_int_resource_invalid_value_returns_zero():
    node = SlurmNode.__new__(SlurmNode)
    node._info = {"CPUTot": "not_a_number"}

    result = node._getIntResource("CPUTot")

    assert result == 0


def test_slurm_node_to_yaml_round_trip():
    node = SlurmNode.__new__(SlurmNode)
    node._name = "node1"

    node._info = {
        "NodeName": "node1",
        "Arch": "x86_64",
        "CoresPerSocket": "16",
        "CPUTot": "128",
        "CPUAlloc": "64",
        "RealMemory": "256000",
        "AllocTRES": "cpu=64,mem=128000M,gres/gpu=4",
        "State": "ALLOCATED",
        "Partitions": "qcpu,qgpu",
    }

    result = node.toYaml()
    parsed = yaml.load(result, Loader=yaml.SafeLoader)

    assert parsed == node._info


def test_slurm_node_from_dict_creates_instance_with_expected_data():
    info = {
        "NodeName": "node1",
        "Arch": "x86_64",
        "CPUTot": "128",
        "State": "IDLE",
    }

    node = SlurmNode.fromDict("node1", info)

    assert isinstance(node, SlurmNode)
    assert node._name == "node1"
    assert node._info == info


def test_slurm_node_is_available_to_user_no_state_returns_false():
    node = SlurmNode.__new__(SlurmNode)
    node._name = "node1"
    node._info = {}

    result = node.isAvailableToUser("user1")

    assert result is False


@pytest.mark.parametrize(
    "state,expected",
    [
        ("ALLOCATED", True),
        ("MIXED", True),
        ("IDLE", True),
        ("DOWN", False),
        ("DRAINED", False),
        ("FAIL", False),
        ("FUTURE", False),
        ("INVAL", False),
        ("MAINT", False),
        ("PERFCTRS", False),
        ("POWERED_DOWN", False),
        ("POWERING_DOWN", False),
        ("RESERVED", False),
        ("UNKNOWN", False),
    ],
)
def test_slurm_node_is_available_to_user_various_states(state, expected):
    node = SlurmNode.__new__(SlurmNode)
    node._name = "node1"
    node._info = {"State": state}

    result = node.isAvailableToUser("user1")

    assert result is expected


def make_node_with_info(info: dict[str, str]) -> SlurmNode:
    node = SlurmNode.__new__(SlurmNode)
    node._info = info
    node._name = info.get("NodeName", "node1")
    return node


def test_slurm_node_get_name_returns_name():
    node = make_node_with_info({"NodeName": "node1"})
    assert node.getName() == "node1"


def test_slurm_node_get_ncpus_returns_correct_value():
    node = make_node_with_info({"CPUTot": "128"})
    assert node.getNCPUs() == 128


def test_slurm_node_get_nfree_cpus_computes_difference():
    node = make_node_with_info({"CPUTot": "128", "CPUAlloc": "64"})
    assert node.getNFreeCPUs() == 64


def test_slurm_node_get_ngpus_returns_value_from_tres():
    node = make_node_with_info({"CfgTRES": "cpu=128,mem=250G,gres/gpu=4"})
    assert node.getNGPUs() == 4


def test_slurm_node_get_nfree_gpus_computes_difference():
    node = make_node_with_info(
        {
            "CfgTRES": "cpu=128,mem=250G,gres/gpu=8",
            "AllocTRES": "cpu=64,mem=125G,gres/gpu=2",
        }
    )
    assert node.getNFreeGPUs() == 6


def test_slurm_node_get_cpu_memory_returns_size_object():
    node = make_node_with_info({"RealMemory": "1024"})
    result = node.getCPUMemory()
    assert isinstance(result, Size)
    assert result.value == 1024 * 1024


def test_slurm_node_get_free_cpu_memory_returns_difference():
    node = make_node_with_info({"RealMemory": "1024M", "AllocMem": "512"})
    result = node.getFreeCPUMemory()
    assert isinstance(result, Size)
    assert result.value == (1024 - 512) * 1024


def test_slurm_node_get_gpu_memory_returns_zero():
    node = make_node_with_info({})
    result = node.getGPUMemory()
    assert isinstance(result, Size)
    assert result.value == 0


def test_slurm_node_get_free_gpu_memory_returns_zero():
    node = make_node_with_info({})
    result = node.getFreeGPUMemory()
    assert isinstance(result, Size)
    assert result.value == 0


def test_slurm_node_get_local_scratch_returns_size():
    node = make_node_with_info({"TmpDisk": "5000"})
    result = node.getLocalScratch()
    assert isinstance(result, Size)
    assert result.value == 5000 * 1024


def test_slurm_node_get_free_local_scratch_returns_size():
    node = make_node_with_info({"TmpDisk": "2500M"})
    result = node.getFreeLocalScratch()
    assert isinstance(result, Size)
    assert result.value == 2500 * 1024


def test_slurm_node_get_ssd_scratch_returns_zero():
    node = make_node_with_info({})
    result = node.getSSDScratch()
    assert isinstance(result, Size)
    assert result.value == 0


def test_slurm_node_get_free_ssd_scratch_returns_zero():
    node = make_node_with_info({})
    result = node.getFreeSSDScratch()
    assert isinstance(result, Size)
    assert result.value == 0


def test_slurm_node_get_shared_scratch_returns_zero():
    node = make_node_with_info({})
    result = node.getSharedScratch()
    assert isinstance(result, Size)
    assert result.value == 0


def test_slurm_node_get_free_shared_scratch_returns_zero():
    node = make_node_with_info({})
    result = node.getFreeSharedScratch()
    assert isinstance(result, Size)
    assert result.value == 0


def test_slurm_node_get_properties_returns_list_of_features():
    node = make_node_with_info({"AvailableFeatures": "x86_64,amd,milan"})
    result = node.getProperties()
    assert result == ["x86_64", "amd", "milan"]


def test_slurm_node_get_properties_returns_empty_list_if_missing():
    node = make_node_with_info({})
    result = node.getProperties()
    assert result == []
