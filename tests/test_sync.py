# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from rich.console import Console

from qq_lib.batch.vbs.qqvbs import QQVBS
from qq_lib.core.constants import DATE_FORMAT
from qq_lib.core.error import QQError
from qq_lib.info.informer import QQInformer
from qq_lib.properties.info import QQInfo
from qq_lib.properties.job_type import QQJobType
from qq_lib.properties.resources import QQResources
from qq_lib.properties.states import NaiveState, RealState
from qq_lib.submit.cli import submit
from qq_lib.submit.submitter import QQSubmitter
from qq_lib.sync.cli import sync
from qq_lib.sync.syncer import QQSyncer


@pytest.fixture
def sample_resources():
    return QQResources(ncpus=8, work_dir="scratch_local")


@pytest.fixture
def sample_info(sample_resources):
    return QQInfo(
        batch_system=QQVBS,
        qq_version="0.1.0",
        username="fake_user",
        job_id="12345.fake.server.com",
        job_name="script.sh+025",
        script_name="script.sh",
        queue="default",
        job_type=QQJobType.STANDARD,
        input_machine="fake.machine.com",
        input_dir=Path("/shared/storage/"),
        job_state=NaiveState.RUNNING,
        submission_time=datetime.strptime("2025-09-21 12:00:00", DATE_FORMAT),
        stdout_file="stdout.log",
        stderr_file="stderr.log",
        resources=sample_resources,
        excluded_files=[Path("ignore.txt")],
        command_line=["-q", "default", "script.sh"],
        main_node="random.node.org",
        all_nodes=["random.node.org"],
        work_dir=Path("/scratch/job_12345.fake.server.com"),
    )


@pytest.fixture
def info_file(tmp_path, sample_info):
    path = tmp_path / "job.qqinfo"
    sample_info.toFile(path)
    return path


def test_syncer_initializes(info_file):
    with patch.object(QQInformer, "getRealState", return_value=RealState.RUNNING):
        syncer = QQSyncer(info_file)
        assert syncer._info_file == info_file
        assert syncer.hasDestination() is True


@pytest.mark.parametrize(
    "state,expected",
    [
        (RealState.FINISHED, True),
        (RealState.QUEUED, False),
        (RealState.KILLED, False),
    ],
)
def test_syncer_is_finished(state, expected, tmp_path, sample_info):
    sample_info.toFile(tmp_path / "job.qqinfo")
    syncer = QQSyncer(tmp_path / "job.qqinfo")
    syncer._state = state
    assert syncer.isFinished() is expected


@pytest.mark.parametrize(
    "state,job_exit_code,expected",
    [
        (RealState.KILLED, None, True),
        (RealState.EXITING, None, True),
        (RealState.EXITING, 0, False),
        (RealState.EXITING, 1, False),
        (RealState.FINISHED, None, False),
    ],
)
def test_syncer_is_killed(state, job_exit_code, expected, tmp_path, sample_info):
    sample_info.toFile(tmp_path / "job.qqinfo")
    syncer = QQSyncer(tmp_path / "job.qqinfo")
    syncer._state = state
    if state == RealState.EXITING:
        syncer._informer.info.job_exit_code = job_exit_code
    assert syncer.isKilled() is expected


@pytest.mark.parametrize(
    "state,job_exit_code,expected",
    [
        (RealState.EXITING, 0, True),
        (RealState.EXITING, 1, False),
        (RealState.EXITING, None, False),
        (RealState.FINISHED, 0, False),
    ],
)
def test_syncer_is_exiting_successfully(
    state, job_exit_code, expected, tmp_path, sample_info
):
    sample_info.toFile(tmp_path / "job.qqinfo")
    syncer = QQSyncer(tmp_path / "job.qqinfo")
    syncer._state = state
    if state == RealState.EXITING:
        syncer._informer.info.job_exit_code = job_exit_code
    assert syncer.isExitingSuccessfully() is expected


@pytest.mark.parametrize(
    "state,expected",
    [
        (RealState.QUEUED, True),
        (RealState.BOOTING, True),
        (RealState.WAITING, True),
        (RealState.HELD, True),
        (RealState.FINISHED, False),
        (RealState.KILLED, False),
    ],
)
def test_syncer_is_queued(state, expected, tmp_path, sample_info):
    sample_info.toFile(tmp_path / "job.qqinfo")
    syncer = QQSyncer(tmp_path / "job.qqinfo")
    syncer._state = state
    assert syncer.isQueued() is expected


@pytest.mark.parametrize(
    "input_id,expected",
    [
        ("12345.fake.server.com", True),
        ("12345.other.domain.net", True),
        ("12345", True),
        ("99999.fake.server.com", False),
        ("54321", False),
        ("", False),
    ],
)
def test_is_job_matches_and_mismatches(info_file, input_id, expected):
    informer = QQSyncer(info_file)
    input_id = input_id.strip()
    assert informer.isJob(input_id) == expected


def test_has_destination_true(info_file):
    syncer = QQSyncer(info_file)
    assert syncer.hasDestination() is True


def test_has_destination_false(tmp_path, sample_info):
    sample_info.work_dir = None
    path = tmp_path / "job.qqinfo"
    sample_info.toFile(path)
    syncer = QQSyncer(path)
    assert syncer.hasDestination() is False


def test_print_info_runs(tmp_path, sample_info):
    path = tmp_path / "job.qqinfo"
    sample_info.toFile(path)
    syncer = QQSyncer(path)
    console = Console(record=True)
    with patch.object(QQInformer, "getRealState", return_value=RealState.RUNNING):
        syncer.printInfo(console)
    output = console.export_text()
    assert sample_info.job_id in output


def test_sync_calls_sync_with_exclusions(info_file):
    syncer = QQSyncer(info_file)

    with patch.object(syncer._batch_system, "syncWithExclusions") as mock_sync:
        syncer.sync()
        mock_sync.assert_called_once_with(
            syncer._directory,
            syncer._informer.info.input_dir,
            syncer._host,
            None,
        )


def test_sync_calls_sync_selected(
    info_file,
):
    syncer = QQSyncer(info_file)
    files = ["stdout.log", "stderr.log"]

    with (
        patch.object(syncer._batch_system, "syncSelected") as mock_sync,
    ):
        syncer.sync(files)
        mock_sync.assert_called_once()

        # check that the paths were joined correctly
        called_files = mock_sync.call_args[0][4]
        assert all(syncer._directory in f.parents for f in called_files)


def test_sync_raises_if_no_destination(info_file):
    syncer = QQSyncer(info_file)
    syncer._host = None
    syncer._directory = None

    with pytest.raises(QQError) as exc:
        syncer.sync()
    assert "Host" in str(exc.value) and "work_dir" in str(exc.value)


@pytest.mark.parametrize(
    "sync_args",
    [
        "",
        ["-f", "file1.txt,file2.txt"],
        ["--files", "file1.txt file2.txt"],
        ["--files", "file1.txt:file2.txt"],
    ],
)
def test_command_sync_integration_running(tmp_path, sync_args):
    QQVBS._batch_system.clearJobs()
    runner = CliRunner()
    script_file = tmp_path / "test_script.sh"
    script_file.write_text("#!/bin/bash\necho Hello\n")
    script_file.chmod(0o755)

    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)

        # submit the first job using VBS
        args = ["-q", "default", str(script_file), "--batch-system", "VBS"]
        with (
            patch.object(QQSubmitter, "_hasValidShebang", return_value=True),
            patch("sys.argv", args),
        ):
            result_submit = runner.invoke(
                submit,
                args,
            )
        assert result_submit.exit_code == 0

        # run the job
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)
        job_id = informer.info.job_id
        QQVBS._batch_system.runJob(job_id, freeze=True)
        informer.setRunning(datetime.now(), "node01", ["node01"], tmp_path / "work_dir")
        informer.toFile(info_file)

        # create work dir and some files in workdir
        (tmp_path / "work_dir").mkdir()
        (tmp_path / "work_dir" / "file1.txt").write_text("File number 1.")
        (tmp_path / "work_dir" / "file2.txt").write_text("File number 2.")

        # sync files
        result_sync = runner.invoke(sync, sync_args)
        assert result_sync.exit_code == 0

        # check synced files
        assert (tmp_path / "file1.txt").is_file()
        assert (tmp_path / "file1.txt").read_text() == "File number 1."

        assert (tmp_path / "file2.txt").is_file()
        assert (tmp_path / "file2.txt").read_text() == "File number 2."

        # files should still exist in the working direcotry
        assert (tmp_path / "work_dir" / "file1.txt").is_file()
        assert (tmp_path / "work_dir" / "file1.txt").read_text() == "File number 1."

        assert (tmp_path / "work_dir" / "file2.txt").is_file()
        assert (tmp_path / "work_dir" / "file2.txt").read_text() == "File number 2."


@pytest.mark.parametrize(
    "state",
    [
        RealState.QUEUED,
        RealState.BOOTING,
        RealState.WAITING,
        RealState.FINISHED,
        RealState.EXITING,  # with exit code 0
        RealState.KILLED,  # without destination
    ],
)
def test_command_sync_integration_not_suitable(tmp_path, state):
    QQVBS._batch_system.clearJobs()
    runner = CliRunner()
    script_file = tmp_path / "test_script.sh"
    script_file.write_text("#!/bin/bash\necho Hello\n")
    script_file.chmod(0o755)

    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)

        # submit the first job using VBS
        args = ["-q", "default", str(script_file), "--batch-system", "VBS"]
        with (
            patch.object(QQSubmitter, "_hasValidShebang", return_value=True),
            patch("sys.argv", args),
        ):
            result_submit = runner.invoke(
                submit,
                args,
            )
        assert result_submit.exit_code == 0

        # run the job
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)
        job_id = informer.info.job_id
        QQVBS._batch_system.runJob(job_id, freeze=True)
        informer.setRunning(datetime.now(), "node01", ["node01"], tmp_path / "work_dir")
        if state == RealState.EXITING:
            informer.info.job_exit_code = 0
        elif state == RealState.KILLED:
            informer.info.work_dir = None
        informer.toFile(info_file)

        # create work dir and some files in workdir
        (tmp_path / "work_dir").mkdir()
        (tmp_path / "work_dir" / "file1.txt").write_text("File number 1.")
        (tmp_path / "work_dir" / "file2.txt").write_text("File number 2.")

        # attempt to sync files
        with patch.object(QQInformer, "getRealState", return_value=state):
            result_sync = runner.invoke(sync, "")
            assert result_sync.exit_code == 91

        # files should not be synced
        assert not (tmp_path / "file1.txt").is_file()

        assert not (tmp_path / "file2.txt").is_file()


def test_command_sync_integration_running_selected_files(tmp_path):
    QQVBS._batch_system.clearJobs()
    runner = CliRunner()
    script_file = tmp_path / "test_script.sh"
    script_file.write_text("#!/bin/bash\necho Hello\n")
    script_file.chmod(0o755)

    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)

        # submit the first job using VBS
        args = ["-q", "default", str(script_file), "--batch-system", "VBS"]
        with (
            patch.object(QQSubmitter, "_hasValidShebang", return_value=True),
            patch("sys.argv", args),
        ):
            result_submit = runner.invoke(
                submit,
                args,
            )
        assert result_submit.exit_code == 0

        # run the job
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)
        job_id = informer.info.job_id
        QQVBS._batch_system.runJob(job_id, freeze=True)
        informer.setRunning(datetime.now(), "node01", ["node01"], tmp_path / "work_dir")
        informer.toFile(info_file)

        # create work dir and some files in workdir
        (tmp_path / "work_dir").mkdir()
        (tmp_path / "work_dir" / "file1.txt").write_text("File number 1.")
        (tmp_path / "work_dir" / "file2.txt").write_text("File number 2.")

        # sync files
        result_sync = runner.invoke(sync, ["-f", "file1.txt"])
        assert result_sync.exit_code == 0

        # check synced files
        assert (tmp_path / "file1.txt").is_file()
        assert (tmp_path / "file1.txt").read_text() == "File number 1."

        assert not (tmp_path / "file2.txt").is_file()

        # files should still exist in the working direcotry
        assert (tmp_path / "work_dir" / "file1.txt").is_file()
        assert (tmp_path / "work_dir" / "file1.txt").read_text() == "File number 1."

        assert (tmp_path / "work_dir" / "file2.txt").is_file()
        assert (tmp_path / "work_dir" / "file2.txt").read_text() == "File number 2."
