# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import stat
from datetime import datetime
from pathlib import Path
from time import sleep
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.batch import BatchOperationResult
from qq_lib.constants import DATE_FORMAT
from qq_lib.error import QQError
from qq_lib.info import QQInfo, QQInformer
from qq_lib.kill import QQKiller, kill
from qq_lib.resources import QQResources
from qq_lib.states import BatchState, NaiveState, RealState
from qq_lib.submit import QQSubmitter, submit
from qq_lib.vbs import QQVBS


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


def test_lock_file_removes_write_permissions(tmp_path):
    f = tmp_path / "job.qqinfo"
    f.write_text("dummy content")

    f.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)
    mode_before = f.stat().st_mode
    assert mode_before & stat.S_IWUSR  # user write
    assert mode_before & stat.S_IWGRP  # group write

    killer = object.__new__(QQKiller)
    killer._lockFile(f)

    mode_after = f.stat().st_mode

    # assert that write permissions were removed
    assert not (mode_after & stat.S_IWUSR)
    assert not (mode_after & stat.S_IWGRP)
    assert not (mode_after & stat.S_IWOTH)

    # but read permissions should remain
    assert mode_after & stat.S_IRUSR
    assert mode_after & stat.S_IRGRP


def test_update_info_file_writes_and_locks(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"

    informer = QQInformer(sample_info)
    informer.toFile(info_file)

    killer = object.__new__(QQKiller)
    killer._info_file = info_file
    killer._informer = QQInformer.fromFile(info_file)

    killer.updateInfoFile()

    reloaded = QQInformer.fromFile(info_file)

    # check that job_state is KILLED
    assert reloaded.info.job_state == NaiveState.KILLED

    # check that completion_time is set to a datetime
    assert isinstance(reloaded.info.completion_time, datetime)

    # check that file is locked (no write permission for user/group/others)
    mode = info_file.stat().st_mode
    assert not (mode & stat.S_IWUSR)
    assert not (mode & stat.S_IWGRP)
    assert not (mode & stat.S_IWOTH)


@pytest.mark.parametrize(
    "state,forced,expected",
    [
        (RealState.QUEUED, True, True),
        (RealState.QUEUED, False, True),
        (RealState.HELD, True, True),
        (RealState.HELD, False, True),
        (RealState.SUSPENDED, True, True),
        (RealState.SUSPENDED, False, True),
        (RealState.WAITING, True, True),
        (RealState.WAITING, False, True),
        (RealState.RUNNING, True, True),
        (RealState.RUNNING, False, False),
        (RealState.BOOTING, True, True),
        (RealState.BOOTING, False, True),
        (RealState.KILLED, True, False),
        (RealState.KILLED, False, False),
        (RealState.FAILED, True, False),
        (RealState.FAILED, False, False),
        (RealState.FINISHED, True, False),
        (RealState.FINISHED, False, False),
        (RealState.IN_AN_INCONSISTENT_STATE, True, False),
        (RealState.IN_AN_INCONSISTENT_STATE, False, False),
        (RealState.UNKNOWN, True, False),
        (RealState.UNKNOWN, False, False),
    ],
)
def test_should_update_info_file(state, forced, expected):
    killer = object.__new__(QQKiller)
    killer._state = state
    killer._forced = forced

    assert killer.shouldUpdateInfoFile() == expected


@pytest.mark.parametrize(
    "state,forced,expected",
    [
        (RealState.QUEUED, True, True),
        (RealState.QUEUED, False, True),
        (RealState.HELD, True, True),
        (RealState.HELD, False, True),
        (RealState.SUSPENDED, True, True),
        (RealState.SUSPENDED, False, True),
        (RealState.WAITING, True, True),
        (RealState.WAITING, False, True),
        (RealState.RUNNING, True, True),
        (RealState.RUNNING, False, True),
        (RealState.BOOTING, True, True),
        (RealState.BOOTING, False, True),
        (RealState.KILLED, True, True),
        (RealState.KILLED, False, False),
        (RealState.FAILED, True, True),
        (RealState.FAILED, False, False),
        (RealState.FINISHED, True, True),
        (RealState.FINISHED, False, False),
        (RealState.IN_AN_INCONSISTENT_STATE, True, True),
        (RealState.IN_AN_INCONSISTENT_STATE, False, True),
        (RealState.UNKNOWN, True, True),
        (RealState.UNKNOWN, False, True),
    ],
)
def test_should_terminate_explicit(state, forced, expected):
    killer = object.__new__(QQKiller)
    killer._state = state
    killer._forced = forced

    assert killer.shouldTerminate() == expected


@pytest.mark.parametrize("forced", [True, False])
@pytest.mark.parametrize("success", [True, False])
def test_terminate(forced, success):
    # create a dummy QQInformer with a fake batch system
    informer_mock = MagicMock()
    informer_mock.info.job_id = "12345"

    batch_mock = MagicMock()
    method_name = "jobKillForce" if forced else "jobKill"

    if success:
        result = BatchOperationResult.success()
    else:
        result = BatchOperationResult.error(1, "fail")

    setattr(batch_mock, method_name, MagicMock(return_value=result))

    killer = object.__new__(QQKiller)
    killer._forced = forced
    killer._informer = informer_mock
    killer._batch_system = batch_mock

    if success:
        # should not raise
        killer.terminate()
        getattr(batch_mock, method_name).assert_called_once_with("12345")
    else:
        # should raise QQError
        with pytest.raises(QQError, match="Could not kill the job: fail"):
            killer.terminate()
        getattr(batch_mock, method_name).assert_called_once_with("12345")


@pytest.mark.parametrize("forced", [False, True])
def test_kill_queued_integration(tmp_path, forced):
    QQVBS._batch_system.clearJobs()
    runner = CliRunner()
    script_file = tmp_path / "test_script.sh"
    script_file.write_text("#!/bin/bash\nsleep 5\n")
    script_file.chmod(0o755)

    # submit the job using VBSS
    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)

        with patch.object(QQSubmitter, "_hasValidShebang", return_value=True):
            result_submit = runner.invoke(
                submit,
                ["default", str(script_file), "--batch-system", "VBS"],
            )
        assert result_submit.exit_code == 0

        result_kill = runner.invoke(kill, ["--force"] if forced else ["-y"])

        assert result_kill.exit_code == 0

        # check that the qq info file exists and is updated
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)

        assert informer.info.job_state == NaiveState.KILLED
        assert isinstance(informer.info.completion_time, datetime)

        # check that the VBS job is marked finished
        job_id = informer.info.job_id
        job = QQVBS._batch_system.jobs[job_id]
        assert job.state == BatchState.FINISHED


@pytest.mark.parametrize("forced", [False, True])
def test_kill_booting_integration(tmp_path, forced):
    QQVBS._batch_system.clearJobs()
    runner = CliRunner()
    script_file = tmp_path / "test_script.sh"
    script_file.write_text("#!/bin/bash\nsleep 5\n")
    script_file.chmod(0o755)

    # submit the job using VBSS
    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)

        with patch.object(QQSubmitter, "_hasValidShebang", return_value=True):
            result_submit = runner.invoke(
                submit,
                ["default", str(script_file), "--batch-system", "VBS"],
            )
        assert result_submit.exit_code == 0

        # run the job
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)
        job_id = informer.info.job_id
        QQVBS._batch_system.runJob(job_id)

        result_kill = runner.invoke(kill, ["--force"] if forced else ["-y"])

        assert result_kill.exit_code == 0

        # check that the qq info file exists and is updated
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)

        assert informer.info.job_state == NaiveState.KILLED
        assert isinstance(informer.info.completion_time, datetime)

        # check that the VBS job is marked finished
        job_id = informer.info.job_id
        job = QQVBS._batch_system.jobs[job_id]
        assert job.state == BatchState.FINISHED


@pytest.mark.parametrize("forced", [False, True])
def test_kill_running_integration(tmp_path, forced):
    QQVBS._batch_system.clearJobs()
    runner = CliRunner()
    script_file = tmp_path / "test_script.sh"
    script_file.write_text("#!/bin/bash\nsleep 5\n")
    script_file.chmod(0o755)

    # submit the job using VBSS
    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)

        with patch.object(QQSubmitter, "_hasValidShebang", return_value=True):
            result_submit = runner.invoke(
                submit,
                ["default", str(script_file), "--batch-system", "VBS"],
            )
        assert result_submit.exit_code == 0

        # run the job
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)
        job_id = informer.info.job_id
        QQVBS._batch_system.runJob(job_id)

        # set the job as running in qq info
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)
        informer.setRunning(datetime.now(), "main.node.org", tmp_path)
        informer.toFile(info_file)

        result_kill = runner.invoke(kill, ["--force"] if forced else ["-y"])

        assert result_kill.exit_code == 0

        # check that the qq info file exists and is updated if forced
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)

        if forced:
            assert informer.info.job_state == NaiveState.KILLED
            assert isinstance(informer.info.completion_time, datetime)
        else:
            assert informer.info.job_state == NaiveState.RUNNING
            assert informer.info.completion_time is None

        # check that the VBS job is marked finished
        job_id = informer.info.job_id
        job = QQVBS._batch_system.jobs[job_id]
        assert job.state == BatchState.FINISHED


@pytest.mark.parametrize("forced", [False, True])
def test_kill_finished_integration(tmp_path, forced):
    QQVBS._batch_system.clearJobs()
    runner = CliRunner()
    script_file = tmp_path / "test_script.sh"
    script_file.write_text("#!/bin/bash\necho Hello\n")
    script_file.chmod(0o755)

    # submit the job using VBSS
    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)

        with patch.object(QQSubmitter, "_hasValidShebang", return_value=True):
            result_submit = runner.invoke(
                submit,
                ["default", str(script_file), "--batch-system", "VBS"],
            )
        assert result_submit.exit_code == 0

        # run the job
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)
        job_id = informer.info.job_id
        QQVBS._batch_system.runJob(job_id)

        sleep(0.3)

        # set the job as finished in qq info
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)
        informer.setFinished(datetime.now())
        informer.toFile(info_file)

        result_kill = runner.invoke(kill, ["--force"] if forced else ["-y"])

        # check that the qq info file exists and is not updated
        assert result_kill.exit_code == 91
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)
        informer.info.job_state == NaiveState.FINISHED

        # check that the VBS job is marked finished
        job_id = informer.info.job_id
        job = QQVBS._batch_system.jobs[job_id]
        assert job.state == BatchState.FINISHED
