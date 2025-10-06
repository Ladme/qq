# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import getpass
import os
import socket
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.batch import QQBatchMeta
from qq_lib.constants import (
    DATE_FORMAT,
    QQ_INFO_SUFFIX,
    QQ_OUT_SUFFIX,
    STDERR_SUFFIX,
    STDOUT_SUFFIX,
)
from qq_lib.error import QQError
from qq_lib.info import QQInfo, QQInformer
from qq_lib.job_type import QQJobType
from qq_lib.loop import QQLoopInfo
from qq_lib.pbs import QQPBS
from qq_lib.resources import QQResources
from qq_lib.states import NaiveState, RealState
from qq_lib.submit import QQSubmitter, submit
from qq_lib.vbs import QQVBS


@pytest.fixture(autouse=True)
def register():
    QQBatchMeta.register(QQPBS)
    QQBatchMeta.register(QQVBS)


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
    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        True,
    )
    assert submitter._script == script_with_shebang
    assert submitter._resources == sample_resources
    assert submitter._info_file.suffix != ""


def test_submitter_init_nonexistent_file(tmp_path, sample_resources):
    os.chdir(tmp_path)
    script = tmp_path / "missing.sh"
    with pytest.raises(QQError, match="does not exist"):
        QQSubmitter(
            QQVBS,
            "default",
            script,
            QQJobType.STANDARD,
            sample_resources,
            None,
            [],
            ["-q", "default"],
            True,
        )


def test_submitter_init_script_not_in_cwd(script_with_shebang, sample_resources):
    # stay outside tmp_path
    Path.cwd()
    with pytest.raises(QQError, match="is not in the submission directory"):
        QQSubmitter(
            QQVBS,
            "default",
            script_with_shebang,
            QQJobType.STANDARD,
            sample_resources,
            None,
            [],
            ["-q", "default", str(script_with_shebang)],
            True,
        )


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
        QQSubmitter(
            QQVBS,
            "default",
            script_with_shebang,
            QQJobType.STANDARD,
            sample_resources,
            None,
            [],
            ["-q", "default", str(script_with_shebang)],
            True,
        )


def test_submitter_init_invalid_shebang(
    script_invalid_shebang, sample_resources, tmp_path
):
    os.chdir(tmp_path)
    with pytest.raises(QQError, match="invalid shebang"):
        QQSubmitter(
            QQVBS,
            "default",
            script_invalid_shebang,
            QQJobType.STANDARD,
            sample_resources,
            None,
            [],
            ["-q", "default", str(script_invalid_shebang)],
            True,
        )


def test_submitter_submit_success(script_with_shebang, sample_resources, tmp_path):
    os.chdir(tmp_path)
    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        True,
    )
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
    def fake_jobSubmit(_res, _queue, _script, _name):
        raise QQError("Failed to submit")

    monkeypatch.setattr(QQVBS, "jobSubmit", fake_jobSubmit)

    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        True,
    )
    with pytest.raises(QQError, match="Failed to submit"):
        submitter.submit()


def test_qq_files_present_detects_suffix(
    script_with_shebang, sample_resources, tmp_path
):
    os.chdir(tmp_path)
    f = tmp_path / "dummy.qqout"
    f.write_text("something")
    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        True,
    )
    assert submitter._qqFilesPresent()


def test_set_env_vars_sets_variables(script_with_shebang, sample_resources, tmp_path):
    os.chdir(tmp_path)
    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        True,
    )
    submitter._setEnvVars()
    assert os.environ.get("QQ_ENV_SET") == "true"
    assert os.environ.get("QQ_INFO") == str(submitter._info_file)


def test_has_valid_shebang(script_with_shebang, sample_resources, tmp_path):
    os.chdir(tmp_path)
    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        True,
    )
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


@pytest.mark.parametrize("interactive", [True, False])
def test_guard_or_clear_no_files(
    script_with_shebang, sample_resources, tmp_path, interactive
):
    os.chdir(tmp_path)
    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        interactive,
    )

    # no raise
    submitter.guardOrClear()


def test_guard_or_clear_invalid_files_user_clears(
    script_with_shebang, sample_resources, tmp_path
):
    os.chdir(tmp_path)
    files = make_dummy_files(tmp_path)

    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        True,
    )

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

    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        True,
    )

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
@pytest.mark.parametrize("interactive", [True, False])
def test_guard_or_clear_active_or_finished_always_raises(
    script_with_shebang, sample_resources, tmp_path, state, interactive
):
    os.chdir(tmp_path)
    make_dummy_files(tmp_path)

    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        interactive,
    )

    informer_mock = MagicMock()
    informer_mock.getRealState.return_value = state

    with (
        patch.object(QQInformer, "fromFile", return_value=informer_mock),
        pytest.raises(QQError, match="Detected qq runtime files"),
    ):
        submitter.guardOrClear()


@pytest.mark.parametrize("state", list(RealState))
def test_guard_or_clear_non_interactive_any_state_always_raises(
    script_with_shebang, sample_resources, tmp_path, state
):
    os.chdir(tmp_path)
    make_dummy_files(tmp_path)

    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        False,
    )

    informer_mock = MagicMock()
    informer_mock.getRealState.return_value = state

    with (
        patch.object(QQInformer, "fromFile", return_value=informer_mock),
        pytest.raises(QQError, match="Detected qq runtime files"),
    ):
        submitter.guardOrClear()


@pytest.mark.parametrize("interactive", [True, False])
def test_guard_or_clear_multiple_combination_of_states_always_raises(
    script_with_shebang, sample_resources, tmp_path, interactive
):
    os.chdir(tmp_path)
    make_dummy_files(tmp_path)

    for file in ["job2.qqinfo", "job3.qqinfo"]:
        info_file = tmp_path / file
        info_file.write_text("dummy")

    submitter = QQSubmitter(
        QQVBS,
        "default",
        script_with_shebang,
        QQJobType.STANDARD,
        sample_resources,
        None,
        [],
        ["-q", "default", str(script_with_shebang)],
        interactive,
    )

    informer_mock = MagicMock()
    informer_mock.getRealState.side_effect = [
        RealState.FAILED,
        RealState.KILLED,
        RealState.RUNNING,
    ]

    with (
        patch.object(QQInformer, "fromFile", return_value=informer_mock),
        pytest.raises(QQError, match="Detected qq runtime files"),
    ):
        submitter.guardOrClear()


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


@pytest.fixture
def resources():
    return QQResources(ncpus=4, work_dir="scratch_local")


@pytest.fixture
def base_info(resources, tmp_path):
    return QQInfo(
        batch_system=QQPBS,
        qq_version="0.1.0",
        username="fake_user",
        job_id="12345.server",
        job_name="job.sh+001",
        queue="default",
        script_name="job.sh",
        job_type=QQJobType.STANDARD,
        input_machine="fake.machine.com",
        job_dir=tmp_path,
        job_state=NaiveState.FINISHED,
        submission_time=datetime.strptime("2025-09-21 12:00:00", DATE_FORMAT),
        stdout_file="stdout.log",
        stderr_file="stderr.log",
        resources=resources,
        excluded_files=[],
        command_line=["-q", "default", "job.sh"],
        work_dir=tmp_path / "work",
    )


def make_submitter(loop_info=None):
    sub = object.__new__(QQSubmitter)
    sub._info_file = Path("dummy.qqinfo")
    sub._loop_info = loop_info
    return sub


def test_should_skip_clear_no_info_file(tmp_path):
    submitter = make_submitter(QQLoopInfo(1, 5, tmp_path, "job%04d", current=2))
    os.chdir(tmp_path)

    assert submitter._shouldSkipClear() is False


def test_should_skip_clear_multiple_info_files(tmp_path, base_info):
    file1 = tmp_path / "a.qqinfo"
    file2 = tmp_path / "b.qqinfo"
    base_info.toFile(file1)
    base_info.toFile(file2)
    submitter = make_submitter(QQLoopInfo(1, 5, tmp_path, "job%04d", current=2))
    os.chdir(tmp_path)

    assert submitter._shouldSkipClear() is False


def test_should_skip_clear_single_info_not_loop(tmp_path, base_info):
    file1 = tmp_path / "single.qqinfo"
    base_info.toFile(file1)  # not a loop job
    submitter = make_submitter(QQLoopInfo(1, 5, tmp_path, "job%04d", current=2))
    os.chdir(tmp_path)

    assert submitter._shouldSkipClear() is False


def test_should_skip_clear_loop_wrong_cycle(tmp_path, base_info):
    loop_info = QQLoopInfo(1, 5, tmp_path, "job%04d", current=1)
    base_info.loop_info = loop_info
    file1 = tmp_path / "loop.qqinfo"
    base_info.toFile(file1)

    submitter = make_submitter(QQLoopInfo(1, 5, tmp_path, "job%04d", current=3))
    os.chdir(tmp_path)

    # wrong cycle (file is current=1, submitter expects previous=2)
    assert submitter._shouldSkipClear() is False


def test_should_skip_clear_loop_running(tmp_path, base_info):
    loop_info = QQLoopInfo(1, 5, tmp_path, "job%04d", current=2)
    base_info.loop_info = loop_info
    base_info.job_state = NaiveState.RUNNING
    file1 = tmp_path / "loop.qqinfo"
    base_info.toFile(file1)

    submitter = make_submitter(QQLoopInfo(1, 5, tmp_path, "job%04d", current=3))
    os.chdir(tmp_path)

    # previous job is running
    assert submitter._shouldSkipClear() is False


def test_should_skip_clear_loop_correct_previous_cycle(tmp_path, base_info):
    loop_info = QQLoopInfo(1, 5, tmp_path, "job%04d", current=1)
    base_info.loop_info = loop_info
    file1 = tmp_path / "loop.qqinfo"
    base_info.toFile(file1)

    submitter = make_submitter(QQLoopInfo(1, 5, tmp_path, "job%04d", current=2))
    os.chdir(tmp_path)

    assert submitter._shouldSkipClear() is True


def test_should_skip_clear_invalid_info_file(tmp_path):
    bad_file = tmp_path / "bad.qqinfo"
    bad_file.write_text("not: valid: yaml: [")
    submitter = make_submitter(QQLoopInfo(1, 5, tmp_path, "job%04d", current=2))
    os.chdir(tmp_path)

    # invalid YAML should be caught, returning False
    assert submitter._shouldSkipClear() is False
