# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import pytest

from qq_lib.core.error import QQError
from qq_lib.properties.job_type import QQJobType


def test_str_method():
    assert str(QQJobType.STANDARD) == "standard"
    assert str(QQJobType.LOOP) == "loop"


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("standard", QQJobType.STANDARD),
        ("STANDARD", QQJobType.STANDARD),
        ("sTaNdArD", QQJobType.STANDARD),
        ("loop", QQJobType.LOOP),
        ("LOOP", QQJobType.LOOP),
        ("LoOp", QQJobType.LOOP),
    ],
)
def test_fromStr_valid(input_str, expected):
    assert QQJobType.fromStr(input_str) == expected


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
def test_fromStr_invalid_raises_QQError(invalid_str):
    with pytest.raises(QQError) as excinfo:
        QQJobType.fromStr(invalid_str)
    assert invalid_str in str(excinfo.value)
