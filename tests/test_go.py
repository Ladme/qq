# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import socket
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.batch import QQBatchMeta
from qq_lib.constants import DATE_FORMAT
from qq_lib.error import QQError
from qq_lib.go import QQGoer, go
from qq_lib.info import QQInfo, QQInformer
from qq_lib.pbs import QQPBS
from qq_lib.resources import QQResources
from qq_lib.states import NaiveState, RealState
from qq_lib.vbs import QQVBS


@pytest.fixture(autouse=True)
def register():
    QQBatchMeta.register(QQPBS)
    QQBatchMeta.register(QQVBS)


@pytest.fixture
def sample_resources():
    return QQResources(ncpus=8, work_dir="scratch_local")


@pytest.fixture
def sample_info(tmp_path, sample_resources):
    # Create a fake main_node and work_dir
    main_node = tmp_path / "fake_node"
    work_dir = main_node / "job_12345.fake.server.com"
    work_dir.mkdir(parents=True)

    return QQInfo(
        batch_system=QQVBS,
        qq_version="0.1.0",
        username="fake_user",
        job_id="12345.fake.server.com",
        job_name="script.sh+025",
        script_name="script.sh",
        job_type="standard",
        input_machine="fake.machine.com",
        job_dir=tmp_path / "job_dir",
        job_state=NaiveState.RUNNING,
        submission_time=datetime.strptime("2025-09-21 12:00:00", DATE_FORMAT),
        stdout_file="stdout.log",
        stderr_file="stderr.log",
        resources=sample_resources,
        excluded_files=[Path("ignore.txt")],
        work_dir=work_dir,
        main_node=str(main_node),
    )


def test_set_destination_with_valid_destination():
    goer = QQGoer.__new__(QQGoer)
    goer._informer = MagicMock()
    goer._informer.getDestination.return_value = ("host123", Path("/tmp/workdir"))

    goer._setDestination()

    assert goer._host == "host123"
    assert goer._directory == Path("/tmp/workdir")
    goer._informer.getDestination.assert_called_once()


def test_set_destination_with_none():
    goer = QQGoer.__new__(QQGoer)
    goer._informer = MagicMock()
    goer._informer.getDestination.return_value = None

    goer._setDestination()

    assert goer._host is None
    assert goer._directory is None
    goer._informer.getDestination.assert_called_once()


def test_is_in_work_dir_true(tmp_path, sample_info):
    goer = QQGoer.__new__(QQGoer)
    goer._directory = tmp_path
    goer._host = socket.gethostname()
    goer._informer = QQInformer(sample_info)

    with patch("pathlib.Path.cwd", return_value=tmp_path):
        assert goer._isInWorkDir() is True


def test_is_in_work_dir_false_different_directory(tmp_path, sample_info):
    goer = QQGoer.__new__(QQGoer)
    goer._directory = tmp_path / "other_dir"
    goer._host = socket.gethostname()
    goer._informer = QQInformer(sample_info)

    with patch("pathlib.Path.cwd", return_value=tmp_path):
        assert goer._isInWorkDir() is False


def test_is_in_work_dir_false_different_host(tmp_path, sample_info):
    goer = QQGoer.__new__(QQGoer)
    goer._directory = tmp_path
    goer._host = "fake_host"
    goer._informer = QQInformer(sample_info)

    with patch("pathlib.Path.cwd", return_value=tmp_path):
        assert goer._isInWorkDir() is False


def test_is_in_work_dir_false_directory_none(tmp_path, sample_info):
    goer = QQGoer.__new__(QQGoer)
    goer._directory = None
    goer._host = socket.gethostname()
    goer._informer = QQInformer(sample_info)

    with patch("pathlib.Path.cwd", return_value=tmp_path):
        assert goer._isInWorkDir() is False


def test_is_in_work_dir_true_different_host_job_dir(tmp_path, sample_info):
    goer = QQGoer.__new__(QQGoer)
    goer._directory = tmp_path
    # different host
    goer._host = "fake_host"
    goer._informer = QQInformer(sample_info)
    # set work_dir to job_dir
    goer._informer.info.resources.work_dir = None

    with patch("pathlib.Path.cwd", return_value=tmp_path):
        assert goer._isInWorkDir() is True


def test_navigate_success(sample_info):
    goer = QQGoer.__new__(QQGoer)
    goer._host = "host123"
    goer._directory = Path("/tmp/workdir")
    goer._informer = QQInformer(sample_info)
    goer._batch_system = MagicMock()
    goer._batch_system.navigateToDestination.return_value.exit_code = 0

    goer._navigate()
    goer._batch_system.navigateToDestination.assert_called_once_with(
        "host123", Path("/tmp/workdir")
    )


def test_navigate_failure(sample_info):
    goer = QQGoer.__new__(QQGoer)
    goer._host = "host123"
    goer._directory = Path("/tmp/workdir")
    goer._informer = QQInformer(sample_info)
    goer._batch_system = QQVBS

    with pytest.raises(QQError, match="Could not reach 'host123:/tmp/workdir'"):
        goer._navigate()


def test_navigate_no_host(sample_info):
    goer = QQGoer.__new__(QQGoer)
    goer._host = None
    goer._directory = Path("/tmp/workdir")
    goer._batch_system = MagicMock()
    goer._informer = QQInformer(sample_info)

    with pytest.raises(QQError, match="Host.*work_dir.*not defined"):
        goer._navigate()


def test_navigate_no_directory(sample_info):
    goer = QQGoer.__new__(QQGoer)
    goer._host = "host123"
    goer._directory = None
    goer._batch_system = MagicMock()
    goer._informer = QQInformer(sample_info)

    with pytest.raises(QQError, match="Host.*work_dir.*not defined"):
        goer._navigate()


@pytest.mark.parametrize("state", list(RealState))
@pytest.mark.parametrize("in_workdir", [True, False])
def test_check_and_navigate(tmp_path, state, in_workdir, sample_info):
    print(state, in_workdir)
    goer = QQGoer.__new__(QQGoer)

    goer._state = state
    goer._directory = tmp_path
    goer._host = socket.gethostname()
    goer._batch_system = MagicMock()
    goer._info_file = tmp_path / "dummy.qqinfo"
    goer._wait_time = 0.1
    goer._informer = QQInformer(sample_info)

    goer._navigate = MagicMock()

    # for queued states, break the loop immediately
    def fake_update():
        goer._state = RealState.RUNNING

    goer.update = fake_update

    cwd_patch = tmp_path if in_workdir else tmp_path / "other_dir"
    with patch("pathlib.Path.cwd", return_value=cwd_patch):
        if state == RealState.FINISHED and not in_workdir:
            with pytest.raises(QQError):
                goer.checkAndNavigate()
        else:
            goer.checkAndNavigate()

    if state == RealState.FINISHED and not in_workdir:
        # should raise so we check nothing
        return
    if in_workdir:
        # already in workdir - navigate not called
        assert goer._navigate.call_count == 0
    else:
        # else navigate called once
        assert goer._navigate.call_count == 1


def test_go_command_success(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    sample_info.toFile(info_file)

    runner = CliRunner()
    with patch.object(QQInformer, "getRealState", return_value=RealState.RUNNING):
        os.chdir(tmp_path)
        result = runner.invoke(go)

        assert result.exit_code == 0
        assert Path.cwd().resolve() == sample_info.work_dir.resolve()


def test_go_command_success_already_in_workdir(sample_info):
    info_file = sample_info.work_dir.resolve() / "job.qqinfo"
    sample_info.toFile(info_file)

    runner = CliRunner()
    with patch.object(QQInformer, "getRealState", return_value=RealState.RUNNING):
        os.chdir(sample_info.work_dir.resolve())
        result = runner.invoke(go)

        assert result.exit_code == 0
        assert Path.cwd().resolve() == sample_info.work_dir.resolve()


def test_go_command_failure_finished(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    sample_info.toFile(info_file)

    runner = CliRunner()
    with patch.object(QQInformer, "getRealState", return_value=RealState.FINISHED):
        os.chdir(tmp_path)
        result = runner.invoke(go)

        assert result.exit_code == 91
        assert Path.cwd().resolve() == tmp_path.resolve()


def test_go_command_failure_missing_path(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    sample_info.toFile(info_file)

    Path.rmdir(sample_info.work_dir.resolve())

    runner = CliRunner()
    with patch.object(QQInformer, "getRealState", return_value=RealState.FAILED):
        os.chdir(tmp_path)
        result = runner.invoke(go)

        assert result.exit_code == 91
        assert Path.cwd().resolve() == tmp_path.resolve()
