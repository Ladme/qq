# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os

import pytest
from click.testing import CliRunner

from qq_lib.batch.interface import QQBatchMeta
from qq_lib.batch.pbs import QQPBS
from qq_lib.batch.vbs import QQVBS
from qq_lib.core.constants import QQ_INFO_SUFFIX
from qq_lib.submit import submit


@pytest.fixture(autouse=True)
def register():
    QQBatchMeta.register(QQPBS)
    QQBatchMeta.register(QQVBS)


@pytest.fixture
def script_with_shebang(tmp_path):
    script = tmp_path / "test.sh"
    script.write_text("#!/usr/bin/env -S qq run\n echo 'hello world'\n")
    script.chmod(script.stat().st_mode | 0o111)
    return script


@pytest.fixture
def script_invalid_shebang(tmp_path):
    script = tmp_path / "bad.sh"
    script.write_text("#!/bin/bash\necho 'nope'\n")
    script.chmod(script.stat().st_mode | 0o111)
    return script


def test_submit_success(tmp_path, script_with_shebang):
    os.chdir(tmp_path)

    runner = CliRunner()

    result = runner.invoke(
        submit, ["-q", "default", script_with_shebang.name, "--batch-system", "VBS"]
    )

    print(result.stderr)
    assert result.exit_code == 0

    info_file = script_with_shebang.with_suffix(QQ_INFO_SUFFIX)
    assert info_file.exists()


def test_submit_missing_script(tmp_path):
    os.chdir(tmp_path)

    runner = CliRunner()
    result = runner.invoke(
        submit, ["--queue", "default", "missing.sh", "--batch-system", "VBS"]
    )

    assert result.exit_code == 91
    assert "does not exist" in result.output


def test_submit_invalid_shebang(tmp_path, script_invalid_shebang):
    os.chdir(tmp_path)

    runner = CliRunner()

    result = runner.invoke(
        submit, ["-q", "default", script_invalid_shebang.name, "--batch-system", "VBS"]
    )

    assert result.exit_code == 91
    assert "invalid shebang" in result.output
