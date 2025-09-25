# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import getpass
import os
import socket
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.constants import QQ_INFO_SUFFIX, QQ_OUT_SUFFIX, STDERR_SUFFIX, STDOUT_SUFFIX
from qq_lib.error import QQError
from qq_lib.info import QQInfo, QQInformer
from qq_lib.resources import QQResources
from qq_lib.states import NaiveState, RealState
from qq_lib.submit import QQSubmitter, submit
from qq_lib.vbs import QQVBS


@pytest.fixture
def sample_resources():
    return QQResources(ncpus=8, work_dir="scratch_local")


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


def test_submitter_init_valid(script_with_shebang, sample_resources, tmp_path):
    os.chdir(tmp_path)
    submitter = QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)
    assert submitter._script == script_with_shebang
    assert submitter._resources == sample_resources
    assert submitter._info_file.suffix != ""


def test_submitter_init_nonexistent_file(tmp_path, sample_resources):
    os.chdir(tmp_path)
    script = tmp_path / "missing.sh"
    with pytest.raises(QQError, match="does not exist"):
        QQSubmitter(QQVBS, "default", script, sample_resources)


def test_submitter_init_script_not_in_cwd(script_with_shebang, sample_resources):
    # stay outside tmp_path
    Path.cwd()
    with pytest.raises(QQError, match="is not in the submission directory"):
        QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)


def test_submitter_init_script_not_in_cwd_matching(
    script_with_shebang, sample_resources, tmp_path
):
    # submission must fail even if there is a script of the same name in the current directory
    inner_dir = tmp_path / "inner"
    inner_dir.mkdir()
    os.chdir(inner_dir)

    script = tmp_path / "inner" / "test.sh"
    script.write_text("#!/bin/bash\necho 'nope'\n")
    script.chmod(script.stat().st_mode | 0o111)

    with pytest.raises(QQError, match="is not in the submission directory"):
        QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)


def test_submitter_init_invalid_shebang(
    script_invalid_shebang, sample_resources, tmp_path
):
    os.chdir(tmp_path)
    with pytest.raises(QQError, match="invalid shebang"):
        QQSubmitter(QQVBS, "default", script_invalid_shebang, sample_resources)


def test_submitter_submit_success(script_with_shebang, sample_resources, tmp_path):
    os.chdir(tmp_path)
    submitter = QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)
    job_id = submitter.submit()

    # check if qq info file was created
    assert submitter._info_file.exists()
    info = QQInfo.fromFile(submitter._info_file)
    assert info.job_id == job_id
    assert info.job_state == NaiveState.QUEUED
    assert info.username == getpass.getuser()
    assert info.input_machine == socket.gethostname()
    assert info.script_name == "test.sh"
    assert info.stdout_file == "test.out"
    assert info.stderr_file == "test.err"


def test_submitter_submit_failure(
    script_with_shebang, sample_resources, tmp_path, monkeypatch
):
    os.chdir(tmp_path)

    # force jobSubmit to raise QQError
    def fake_jobSubmit(_res, _queue, _script):
        raise QQError("Failed to submit")

    monkeypatch.setattr(QQVBS, "jobSubmit", fake_jobSubmit)

    submitter = QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)
    with pytest.raises(QQError, match="Failed to submit"):
        submitter.submit()


def test_qq_files_present_detects_suffix(
    script_with_shebang, sample_resources, tmp_path
):
    os.chdir(tmp_path)
    f = tmp_path / "dummy.qqout"
    f.write_text("something")
    submitter = QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)
    assert submitter._qqFilesPresent()


def test_set_env_vars_sets_variables(script_with_shebang, sample_resources, tmp_path):
    os.chdir(tmp_path)
    submitter = QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)
    submitter._setEnvVars()
    assert os.environ.get("QQ_ENV_SET") == "true"
    assert os.environ.get("QQ_INFO") == str(submitter._info_file)


def test_has_valid_shebang(script_with_shebang, sample_resources, tmp_path):
    os.chdir(tmp_path)
    submitter = QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)
    assert submitter._hasValidShebang(script_with_shebang)


def make_dummy_files(tmp_path):
    """Helper: create dummy qq runtime files in tmp_path."""
    files = [
        tmp_path / f"job{QQ_INFO_SUFFIX}",
        tmp_path / f"job{QQ_OUT_SUFFIX}",
        tmp_path / f"job{STDOUT_SUFFIX}",
        tmp_path / f"job{STDERR_SUFFIX}",
    ]
    for f in files:
        f.write_text("dummy")
    return files


def test_guard_or_clear_no_files(script_with_shebang, sample_resources, tmp_path):
    os.chdir(tmp_path)
    submitter = QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)

    # no raise
    submitter.guardOrClear()


def test_guard_or_clear_invalid_files_user_clears(
    script_with_shebang, sample_resources, tmp_path
):
    os.chdir(tmp_path)
    files = make_dummy_files(tmp_path)

    submitter = QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)

    informer_mock = MagicMock()
    informer_mock.getRealState.return_value = RealState.FAILED

    with (
        patch.object(QQInformer, "fromFile", return_value=informer_mock),
        patch("readchar.readkey", return_value="y"),
    ):
        submitter.guardOrClear()

    # files should be deleted
    for f in files:
        assert not f.exists()


def test_guard_or_clear_invalid_files_user_declines(
    script_with_shebang, sample_resources, tmp_path
):
    os.chdir(tmp_path)
    files = make_dummy_files(tmp_path)

    submitter = QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)

    informer_mock = MagicMock()
    informer_mock.getRealState.return_value = RealState.FAILED

    with (
        patch.object(QQInformer, "fromFile", return_value=informer_mock),
        patch("readchar.readkey", return_value="n"),
        pytest.raises(QQError, match="Submission aborted."),
    ):
        submitter.guardOrClear()

    # files should exist
    for f in files:
        assert f.exists()


@pytest.mark.parametrize(
    "state",
    [
        RealState.QUEUED,
        RealState.RUNNING,
        RealState.FINISHED,
        RealState.WAITING,
        RealState.BOOTING,
    ],
)
def test_guard_or_clear_active_or_finished_always_raises(
    script_with_shebang, sample_resources, tmp_path, state
):
    os.chdir(tmp_path)
    make_dummy_files(tmp_path)

    submitter = QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)

    informer_mock = MagicMock()
    informer_mock.getRealState.return_value = state

    with (
        patch.object(QQInformer, "fromFile", return_value=informer_mock),
        pytest.raises(
            QQError, match="Detected qq runtime files from an active or successful run"
        ),
    ):
        submitter.guardOrClear()


def test_guard_or_clear_multiple_combination_of_states_always_raises(
    script_with_shebang,
    sample_resources,
    tmp_path,
):
    os.chdir(tmp_path)
    make_dummy_files(tmp_path)

    for file in ["job2.qqinfo", "job3.qqinfo"]:
        info_file = tmp_path / file
        info_file.write_text("dummy")

    submitter = QQSubmitter(QQVBS, "default", script_with_shebang, sample_resources)

    informer_mock = MagicMock()
    informer_mock.getRealState.side_effect = [
        RealState.FAILED,
        RealState.KILLED,
        RealState.RUNNING,
    ]

    with (
        patch.object(QQInformer, "fromFile", return_value=informer_mock),
        pytest.raises(
            QQError, match="Detected qq runtime files from an active or successful run"
        ),
    ):
        submitter.guardOrClear()


def test_is_shared_returns_false_for_local(monkeypatch, tmp_path):
    def fake_run(cmd, **kwargs):
        _ = cmd
        _ = kwargs

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    submitter = QQSubmitter.__new__(QQSubmitter)
    assert submitter.isShared(tmp_path) is False


def test_is_shared_returns_true_for_shared(monkeypatch, tmp_path):
    def fake_run(cmd, **kwargs):
        _ = cmd
        _ = kwargs

        class Result:
            returncode = 1

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    submitter = QQSubmitter.__new__(QQSubmitter)
    assert submitter.isShared(tmp_path) is True


def test_is_shared_passes_correct_command(monkeypatch, tmp_path):
    captured = {}

    def fake_run(cmd, **kwargs):
        _ = kwargs
        captured["cmd"] = cmd

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    submitter = QQSubmitter.__new__(QQSubmitter)
    submitter.isShared(tmp_path)

    assert captured["cmd"][0:2] == ["df", "-l"]
    assert Path(captured["cmd"][2]) == tmp_path


def test_submit_success(tmp_path, script_with_shebang):
    os.chdir(tmp_path)

    runner = CliRunner()

    with patch.object(QQSubmitter, "isShared", return_value=True):
        result = runner.invoke(
            submit, ["default", script_with_shebang.name, "--batch-system", "VBS"]
        )

    assert result.exit_code == 0

    info_file = script_with_shebang.with_suffix(QQ_INFO_SUFFIX)
    assert info_file.exists()


def test_submit_missing_script(tmp_path):
    """
    Submitting a non-existent script should fail.
    """
    os.chdir(tmp_path)

    runner = CliRunner()

    with patch.object(QQSubmitter, "isShared", return_value=True):
        result = runner.invoke(
            submit, ["default", "missing.sh", "--batch-system", "VBS"]
        )

    assert result.exit_code == 91
    assert "does not exist" in result.output


def test_submit_invalid_shebang(tmp_path, script_invalid_shebang):
    """
    Submitting a script with invalid shebang should fail.
    """
    os.chdir(tmp_path)

    runner = CliRunner()

    with patch.object(QQSubmitter, "isShared", return_value=True):
        result = runner.invoke(
            submit, ["default", script_invalid_shebang.name, "--batch-system", "VBS"]
        )

    assert result.exit_code == 91
    assert "invalid shebang" in result.output
