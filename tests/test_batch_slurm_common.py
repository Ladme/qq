# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from dataclasses import fields

from qq_lib.batch.slurm.common import (
    default_resources_from_dict,
    parse_slurm_dump_to_dictionary,
)
from qq_lib.properties.resources import Resources
from qq_lib.properties.size import Size


def test_parse_slurm_dump_to_dictionary_parses_single_line():
    text = "JobId=111 JobName=test UserId=user(1001) JobState=RUNNING Partition=gpu"
    result = parse_slurm_dump_to_dictionary(text)
    assert result["JobId"] == "111"
    assert result["JobName"] == "test"
    assert result["UserId"] == "user(1001)"
    assert result["JobState"] == "RUNNING"
    assert result["Partition"] == "gpu"


def test_parse_slurm_dump_to_dictionary_parses_multiline():
    text = "JobId=111\nJobName=test\nUserId=user(1001)\nJobState=RUNNING"
    result = parse_slurm_dump_to_dictionary(text, separator="\n")
    assert result == {
        "JobId": "111",
        "JobName": "test",
        "UserId": "user(1001)",
        "JobState": "RUNNING",
    }


def test_parse_slurm_dump_to_dictionary_ignores_pairs_without_equals():
    text = "JobId=111 InvalidPair JobState=RUNNING"
    result = parse_slurm_dump_to_dictionary(text)
    assert "InvalidPair" not in result
    assert result["JobState"] == "RUNNING"


def test_parse_slurm_dump_to_dictionary_strips_whitespace():
    text = "  JobId=111   JobName=test_job  "
    result = parse_slurm_dump_to_dictionary(text)
    assert result == {"JobId": "111", "JobName": "test_job"}


def test_default_resources_from_dict_converts_and_filters_fields():
    input_dict = {
        "DefMemPerCPU": "4G",
        "DefaultTime": "2-00:00:00",
        "ExtraField": "ignored",
    }
    result = default_resources_from_dict(input_dict)
    assert isinstance(result, Resources)
    assert result.mem_per_cpu == Size.fromString("4gb")
    assert result.walltime == "2d 00:00:00"
    assert not hasattr(result, "ExtraField")


def test_default_resources_from_dict_def_mem_per_cpu_numeric():
    input_dict = {
        "DefMemPerCPU": "4096",
    }
    result = default_resources_from_dict(input_dict)
    assert isinstance(result, Resources)
    assert result.mem_per_cpu == Size.fromString("4096mb")


def test_default_resources_from_dict_def_mem_per_node_numeric():
    input_dict = {
        "DefMemPerNode": "4096",
    }
    result = default_resources_from_dict(input_dict)
    assert isinstance(result, Resources)
    assert result.mem_per_node == Size.fromString("4096mb")


def test_default_resources_from_dict_ignores_unlimited_values():
    input_dict = {
        "DefMemPerCPU": "UNLIMITED",
        "DefMemPerNode": "UNLIMITED",
        "DefaultTime": "UNLIMITED",
    }
    result = default_resources_from_dict(input_dict)
    assert isinstance(result, Resources)
    for f in fields(Resources):
        value = getattr(result, f.name)
        assert value is None
