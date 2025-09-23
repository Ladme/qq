# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import pytest
from qq_lib.batch import BatchOperationResult

def test_error_method():
    result = BatchOperationResult.error(1, "Something went wrong")
    assert result.exit_code == 1
    assert result.error_message == "Something went wrong"
    assert result.success_message is None

def test_success_method():
    result = BatchOperationResult.success("Operation succeeded")
    assert result.exit_code == 0
    assert result.success_message == "Operation succeeded"
    assert result.error_message is None

@pytest.mark.parametrize(
    "code,success_msg,error_msg,expected_exit,expected_success,expected_error",
    [
        (0, "ok", "fail", 0, "ok", None),
        (1, "ok", "fail", 1, None, "fail"),
        (42, None, "critical", 42, None, "critical"),
        (1, "ok", None, 1, None, None),
        (1, None, None, 1, None, None),
        (0, None, None, 0, None, None),
    ]
)
def test_from_exit_code(code, success_msg, error_msg, expected_exit, expected_success, expected_error):
    result = BatchOperationResult.fromExitCode(code, success_msg, error_msg)
    assert result.exit_code == expected_exit
    assert result.success_message == expected_success
    assert result.error_message == expected_error
