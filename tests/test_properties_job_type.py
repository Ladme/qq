# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import pytest

from qq_lib.core.error import QQError
from qq_lib.properties.job_type import JobType


def test_str_method():
    assert str(JobType.STANDARD) == "standard"
    assert str(JobType.LOOP) == "loop"


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("standard", JobType.STANDARD),
        ("STANDARD", JobType.STANDARD),
        ("sTaNdArD", JobType.STANDARD),
        ("loop", JobType.LOOP),
        ("LOOP", JobType.LOOP),
        ("LoOp", JobType.LOOP),
    ],
)
def test_from_str_valid(input_str, expected):
    assert JobType.fromStr(input_str) == expected


@pytest.mark.parametrize(
    "invalid_str",
    [
        "",
        "unknown",
        "job",
        "123",
        "standrd",  # intentional typo
        "looping",
    ],
)
def test_from_str_invalid_raises(invalid_str):
    with pytest.raises(QQError) as excinfo:
        JobType.fromStr(invalid_str)
    assert invalid_str in str(excinfo.value)
