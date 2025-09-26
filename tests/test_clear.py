# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import os
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.batch import QQBatchMeta
from qq_lib.clear import QQClearer, clear
from qq_lib.constants import (
    DATE_FORMAT,
    QQ_OUT_SUFFIX,
    QQ_SUFFIXES,
    STDERR_SUFFIX,
    STDOUT_SUFFIX,
)
from qq_lib.error import QQError
from qq_lib.info import QQInfo, QQInformer
from qq_lib.pbs import QQPBS
from qq_lib.resources import QQResources
from qq_lib.states import BatchState, NaiveState, RealState


@pytest.fixture(autouse=True)
def register():
    QQBatchMeta.register(QQPBS)


def _create_files(tmp_path) -> list[Path]:
    """Create dummy files with qq suffixes."""
    files = []
    for suffix in QQ_SUFFIXES:
        f = tmp_path / f"job{suffix}"
        f.write_text("dummy")
        files.append(f)

    return files


def test_get_qq_files_finds_files(tmp_path):
    files = _create_files(tmp_path)

    clearer = QQClearer(tmp_path)
    found = clearer.getQQFiles()

    # must find all created files
    assert set(found) == set(files)


def test_clear_files_no_files(tmp_path):
    clearer = QQClearer(tmp_path)

    # should not raise or log anything
    clearer.clearFiles([], force=False)


def test_clear_files_deletes_when_should_clear_true(tmp_path):
    files = _create_files(tmp_path)
    clearer = QQClearer(tmp_path)

    with patch.object(QQClearer, "shouldClear", return_value=True):
        clearer.clearFiles(files, force=False)

    for f in files:
        assert not f.exists()


def test_clear_files_raises_when_should_clear_false(tmp_path):
    files = _create_files(tmp_path)
    clearer = QQClearer(tmp_path)

    with (
        patch.object(QQClearer, "shouldClear", return_value=False),
        pytest.raises(QQError, match="may corrupt or delete useful data"),
    ):
        clearer.clearFiles([files], force=False)

    for f in files:
        assert f.exists()


def test_should_clear_true_when_force(tmp_path):
    clearer = QQClearer(tmp_path)
    assert clearer.shouldClear(force=True)


@pytest.mark.parametrize(
    "state",
    [
        RealState.KILLED,
        RealState.FAILED,
        RealState.IN_AN_INCONSISTENT_STATE,
    ],
)
def test_should_clear_true_for_safe_states(tmp_path, state):
    clearer = QQClearer(tmp_path)
    info_file = tmp_path / "job.qqinfo"
    info_file.write_text("dummy")

    informer_mock = MagicMock()
    informer_mock.getRealState.return_value = state

    with (
        patch("qq_lib.common.get_info_file", return_value=info_file),
        patch("qq_lib.info.QQInformer.fromFile", return_value=informer_mock),
    ):
        assert clearer.shouldClear(force=False)


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
def test_should_clear_false_for_active_states(tmp_path, state):
    clearer = QQClearer(tmp_path)
    info_file = tmp_path / "job.qqinfo"
    info_file.write_text("dummy")

    informer_mock = MagicMock()
    informer_mock.getRealState.return_value = state

    with (
        patch("qq_lib.common.get_info_file", return_value=info_file),
        patch("qq_lib.info.QQInformer.fromFile", return_value=informer_mock),
    ):
        assert not clearer.shouldClear(force=False)


def test_should_clear_true_for_multiple_safe_states(tmp_path):
    info_files = ["job1.qqinfo", "job2.qqinfo", "job3.qqinfo"]
    for file in ["job1.qqinfo", "job2.qqinfo", "job3.qqinfo"]:
        info_file = tmp_path / file
        info_file.write_text("dummy")

    clearer = QQClearer(tmp_path)

    informer_mock = MagicMock()
    informer_mock.getRealState.side_effect = [
        RealState.FAILED,
        RealState.KILLED,
        RealState.IN_AN_INCONSISTENT_STATE,
    ]

    with (
        patch("qq_lib.common.get_info_files", return_value=info_files),
        patch.object(QQInformer, "fromFile", return_value=informer_mock),
    ):
        assert clearer.shouldClear(force=False)


def test_should_clear_false_for_multiple_unsafe_states(tmp_path):
    info_files = ["job1.qqinfo", "job2.qqinfo", "job3.qqinfo"]
    for file in ["job1.qqinfo", "job2.qqinfo", "job3.qqinfo"]:
        info_file = tmp_path / file
        info_file.write_text("dummy")

    clearer = QQClearer(tmp_path)

    informer_mock = MagicMock()
    informer_mock.getRealState.side_effect = [
        RealState.RUNNING,
        RealState.FINISHED,
        RealState.QUEUED,
    ]

    with (
        patch("qq_lib.common.get_info_files", return_value=info_files),
        patch.object(QQInformer, "fromFile", return_value=informer_mock),
    ):
        assert not clearer.shouldClear(force=False)


def test_should_clear_false_for_combination_of_safe_unsafe_states(tmp_path):
    info_files = ["job1.qqinfo", "job2.qqinfo", "job3.qqinfo"]
    for file in ["job1.qqinfo", "job2.qqinfo", "job3.qqinfo"]:
        info_file = tmp_path / file
        info_file.write_text("dummy")

    clearer = QQClearer(tmp_path)

    informer_mock = MagicMock()
    informer_mock.getRealState.side_effect = [
        RealState.KILLED,
        RealState.FAILED,
        RealState.QUEUED,
    ]

    with (
        patch("qq_lib.common.get_info_files", return_value=info_files),
        patch.object(QQInformer, "fromFile", return_value=informer_mock),
    ):
        assert not clearer.shouldClear(force=False)


@pytest.fixture
def sample_resources():
    return QQResources(ncpus=8, work_dir="scratch_local")


@pytest.fixture
def sample_info(sample_resources):
    return QQInfo(
        batch_system=QQPBS,
        qq_version="0.1.0",
        username="fake_user",
        job_id="12345.fake.server.com",
        job_name="script.sh+025",
        script_name="script.sh",
        job_type="standard",
        input_machine="fake.machine.com",
        job_dir=Path("/shared/storage/"),
        job_state=NaiveState.RUNNING,
        submission_time=datetime.strptime("2025-09-21 12:00:00", DATE_FORMAT),
        stdout_file="stdout.log",
        stderr_file="stderr.log",
        resources=sample_resources,
        excluded_files=[Path("ignore.txt")],
        work_dir=Path("/scratch/job_12345.fake.server.com"),
    )


def test_should_clear_true_for_multiple_safe_states_and_invalid_file(
    tmp_path, sample_info
):
    for file, state in zip(
        ["job1.qqinfo", "job2.qqinfo"], [NaiveState.KILLED, NaiveState.FAILED]
    ):
        sample_info.job_state = state
        QQInformer(sample_info).toFile(tmp_path / file)

    Path(tmp_path / "jobINVALID.qqinfo").write_text("dummy")

    os.chdir(tmp_path)
    clearer = QQClearer(tmp_path)

    # getBatchState will return BatchState.UNKNOWN which is ignored for these states
    assert clearer.shouldClear(force=False)


def test_should_clear_false_for_multiple_states_and_invalid_file(tmp_path, sample_info):
    for file, state in zip(
        ["job1.qqinfo", "job2.qqinfo"], [NaiveState.FINISHED, NaiveState.FAILED]
    ):
        sample_info.job_state = state
        QQInformer(sample_info).toFile(tmp_path / file)

    Path(tmp_path / "jobINVALID.qqinfo").write_text("dummy")

    os.chdir(tmp_path)
    clearer = QQClearer(tmp_path)

    # getBatchState will return BatchState.UNKNOWN which is ignored for these states
    assert not clearer.shouldClear(force=False)


def _make_runtime_files(
    tmp_path: Path, sample_info: QQInfo, naive_state: NaiveState
) -> list[Path]:
    """
    Create a qqinfo file with the given state and a few other qq files.
    Returns the list of created files.
    """
    files = []
    info_file = tmp_path / "job.qqinfo"
    sample_info.job_state = naive_state
    sample_info.toFile(info_file)
    files.append(info_file)

    # make some other dummy qq files
    for suffix in [QQ_OUT_SUFFIX, STDOUT_SUFFIX, STDERR_SUFFIX]:
        f = tmp_path / f"job{suffix}"
        f.write_text("dummy")
        files.append(f)

    # make dummy non-qq file
    f = tmp_path / "job.result"
    f.write_text("dummy")
    files.append(f)

    return files


def test_clear_running_job_force_false(tmp_path, sample_info):
    # running job -> should NOT clear without --force
    files = _make_runtime_files(tmp_path, sample_info, NaiveState.RUNNING)

    runner = CliRunner()

    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)
        with patch(
            "qq_lib.info.QQInformer.getBatchState", return_value=BatchState.RUNNING
        ):
            result = runner.invoke(clear)

    assert result.exit_code == 91

    # all files should still exist
    for f in files:
        assert f.exists()


def test_clear_failed_job_force_false(tmp_path, sample_info):
    # failed job -> should clear even without --force
    files = _make_runtime_files(tmp_path, sample_info, NaiveState.FAILED)

    runner = CliRunner()

    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)
        with patch(
            "qq_lib.info.QQInformer.getBatchState", return_value=BatchState.FINISHED
        ):
            result = runner.invoke(clear)

    assert result.exit_code == 0

    # qq files should not exist
    for f in files[:-1]:
        assert not f.exists()
    # non-qq file should exist
    assert files[-1].exists()


def test_clear_running_job_force_true(tmp_path, sample_info):
    # running job -> should clear with --force
    files = _make_runtime_files(tmp_path, sample_info, NaiveState.RUNNING)

    runner = CliRunner()

    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)
        with patch(
            "qq_lib.info.QQInformer.getBatchState", return_value=BatchState.RUNNING
        ):
            result = runner.invoke(clear, ["--force"])

    assert result.exit_code == 0

    # qq files should not exist
    for f in files[:-1]:
        assert not f.exists()
    # non-qq file should exist
    assert files[-1].exists()


def test_clear_failed_job_force_true(tmp_path, sample_info):
    # failed job -> should clear
    files = _make_runtime_files(tmp_path, sample_info, NaiveState.FAILED)

    runner = CliRunner()

    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)
        with patch(
            "qq_lib.info.QQInformer.getBatchState", return_value=BatchState.FINISHED
        ):
            result = runner.invoke(clear, ["--force"])

    assert result.exit_code == 0

    # qq files should not exist
    for f in files[:-1]:
        assert not f.exists()
    # non-qq file should exist
    assert files[-1].exists()
