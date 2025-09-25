# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import socket
import subprocess
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.constants import DATE_FORMAT, GUARD, INFO_FILE, SCRATCH_DIR_INNER
from qq_lib.error import QQError
from qq_lib.info import QQInfo, QQInformer
from qq_lib.resources import QQResources
from qq_lib.run import QQRunner, _log_fatal_qq_error, _log_fatal_unexpected_error, run
from qq_lib.states import NaiveState
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
        stdout_file="script.out",
        stderr_file="script.err",
        resources=sample_resources,
        excluded_files=[Path("ignore.txt")],
        work_dir=Path("/scratch/job_12345.fake.server.com"),
    )


def write_info_file_no_scratch_and_set_env_var(tmp_path, sample_info):
    """Helper to write qqinfo file and set env var."""
    # activate qq environment
    os.environ[GUARD] = "true"

    # create a job dir
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    sample_info.job_dir = job_dir
    sample_info.script_name = "script.sh"

    # work in shared storage
    sample_info.resources.work_dir = None

    info_file = job_dir / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)


def test_run_no_scratch_finishes(tmp_path, sample_info):
    write_info_file_no_scratch_and_set_env_var(tmp_path, sample_info)

    script = tmp_path / "job" / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\necho hello\n")

    runner = CliRunner()
    result = runner.invoke(run, [str(tmp_path / "job" / "script.sh")])

    assert result.exit_code == 0
    assert "hello" in (tmp_path / "job" / "script.out").read_text()


def write_info_file_with_scratch_and_set_env_var(monkeypatch, tmp_path, sample_info):
    """Helper to write qqinfo file and set env var."""
    # activate qq environment
    os.environ[GUARD] = "true"

    # create a job dir
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    sample_info.job_dir = job_dir
    sample_info.script_name = "script.sh"

    # create a scratch dir & set a monkeypatch for it
    scratch_dir = tmp_path / "scratch"
    scratch_dir.mkdir()
    monkeypatch.setattr(
        QQVBS,
        "getScratchDir",
        staticmethod(lambda _: scratch_dir),
    )

    info_file = job_dir / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)


def test_run_scratch_finishes(monkeypatch, tmp_path, sample_info):
    write_info_file_with_scratch_and_set_env_var(monkeypatch, tmp_path, sample_info)

    script = tmp_path / "job" / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\necho hello\n")

    runner = CliRunner()
    result = runner.invoke(run, [str(tmp_path / "job" / "script.sh")])

    assert result.exit_code == 0
    assert "hello" in (tmp_path / "job" / "script.out").read_text()
    # scratch should exist
    assert (tmp_path / "scratch").exists()
    # but working directory should not
    assert not (tmp_path / "scratch" / SCRATCH_DIR_INNER).exists()


def test_run_scratch_fails(monkeypatch, tmp_path, sample_info):
    write_info_file_with_scratch_and_set_env_var(monkeypatch, tmp_path, sample_info)

    script = tmp_path / "job" / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\necho hello\nexit 2\n")

    runner = CliRunner()
    result = runner.invoke(run, [str(tmp_path / "job" / "script.sh")])

    assert result.exit_code == 2
    # should not be copied from work directory
    assert (
        "hello" in (tmp_path / "scratch" / SCRATCH_DIR_INNER / "script.out").read_text()
    )


def test_run_scratch_guard_fail(monkeypatch, tmp_path, sample_info):
    write_info_file_with_scratch_and_set_env_var(monkeypatch, tmp_path, sample_info)

    script = tmp_path / "job" / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\necho hello\n")

    # unset guard
    del os.environ[GUARD]

    runner = CliRunner()
    result = runner.invoke(run, [str(tmp_path / "job" / "script.sh")])

    assert result.exit_code == 91


def test_run_scratch_nonexistent_info_file(monkeypatch, tmp_path, sample_info):
    write_info_file_with_scratch_and_set_env_var(monkeypatch, tmp_path, sample_info)

    script = tmp_path / "job" / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\necho hello\n")

    # remove info file
    Path.unlink(tmp_path / "job" / "job.qqinfo")

    runner = CliRunner()
    result = runner.invoke(run, [str(tmp_path / "job" / "script.sh")])

    assert result.exit_code == 92


def write_info_file(path: Path, sample_info: QQInfo):
    """Helper to dump a valid QQInfo into a YAML file."""
    informer = QQInformer(sample_info)
    informer.toFile(path)
    return path


def test_runner_init_ok(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    write_info_file(info_file, sample_info)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()

    assert runner._informer is not None
    assert runner._info_file == info_file
    assert isinstance(runner._informer, QQInformer)


def test_runner_init_env_not_set(monkeypatch):
    monkeypatch.delenv(INFO_FILE, raising=False)

    with pytest.raises(QQError, match="not set"):
        QQRunner()


def test_runner_init_info_file_missing(tmp_path):
    info_file = tmp_path / "missing.qqinfo"
    os.environ[INFO_FILE] = str(info_file)

    with pytest.raises(QQError, match="does not exist"):
        QQRunner()


def test_runner_init_info_file_incomplete(tmp_path, sample_info):
    info_file = tmp_path / "bad.qqinfo"
    # write info file missing a single field
    informer = QQInformer(sample_info)
    # intentionally making info invalid
    informer.info.job_id = None  # ty: ignore[invalid-assignment]
    informer.toFile(info_file)

    os.environ[INFO_FILE] = str(info_file)

    with pytest.raises(QQError, match="Mandatory information missing"):
        QQRunner()


def test_set_up_initializes_runner(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()

    runner.setUp()

    assert runner._job_dir == sample_info.job_dir
    assert runner._batch_system == sample_info.batch_system
    assert runner._use_scratch == sample_info.resources.useScratch()


@pytest.fixture
def runner_with_dirs(tmp_path, sample_info):
    job_dir = tmp_path / "job"
    job_dir.mkdir()

    info_file = job_dir / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()
    runner.setUp()

    Path.unlink(info_file)

    runner._work_dir = tmp_path / "work"
    runner._work_dir.mkdir()
    runner._job_dir = job_dir
    return runner


def test_set_up_work_dir_calls_correct_method(runner_with_dirs):
    runner = runner_with_dirs

    # use scratch
    runner._use_scratch = True
    with (
        patch.object(runner, "_setUpScratchDir") as mock_scratch,
        patch.object(runner, "_setUpSharedDir") as mock_shared,
    ):
        runner.setUpWorkDir()
        mock_scratch.assert_called_once()
        mock_shared.assert_not_called()

    # no scratch
    runner._use_scratch = False
    with (
        patch.object(runner, "_setUpScratchDir") as mock_scratch,
        patch.object(runner, "_setUpSharedDir") as mock_shared,
    ):
        runner.setUpWorkDir()
        mock_shared.assert_called_once()
        mock_scratch.assert_not_called()


def test_execute_script_success(tmp_path, runner_with_dirs):
    runner = runner_with_dirs

    success_script = tmp_path / "success.sh"
    success_script.write_text(
        "#!/usr/bin/env -S qq run\n"
        "echo 'stdout success'\n"
        "echo 'stderr success' >&2\n"
        "exit 0\n"
    )
    success_script.chmod(success_script.stat().st_mode | 0o111)

    os.chdir(tmp_path)

    runner._informer.info.script_name = success_script.name
    runner._informer.info.stdout_file = str(tmp_path / "stdout_success.log")
    runner._informer.info.stderr_file = str(tmp_path / "stderr_success.log")

    ret_code = runner.executeScript()
    assert ret_code == 0

    assert "stdout success" in (tmp_path / "stdout_success.log").read_text()
    assert "stderr success" in (tmp_path / "stderr_success.log").read_text()
    updated_info = QQInfo.fromFile(runner._job_dir / "job.qqinfo")
    assert updated_info.job_state == NaiveState.RUNNING


def test_execute_script_failure(tmp_path, runner_with_dirs):
    runner = runner_with_dirs

    success_script = tmp_path / "success.sh"
    success_script.write_text(
        "#!/usr/bin/env -S qq run\n"
        "echo 'stdout success'\n"
        "echo 'stderr success' >&2\n"
        "exit 1\n"
    )
    success_script.chmod(success_script.stat().st_mode | 0o111)

    os.chdir(tmp_path)

    runner._informer.info.script_name = success_script.name
    runner._informer.info.stdout_file = str(tmp_path / "stdout_success.log")
    runner._informer.info.stderr_file = str(tmp_path / "stderr_success.log")

    ret_code = runner.executeScript()
    assert ret_code == 1

    assert "stdout success" in (tmp_path / "stdout_success.log").read_text()
    assert "stderr success" in (tmp_path / "stderr_success.log").read_text()
    updated_info = QQInfo.fromFile(runner._job_dir / "job.qqinfo")
    assert updated_info.job_state == NaiveState.RUNNING


@pytest.fixture
def runner_with_dirs_and_files(tmp_path, sample_info):
    job_dir = tmp_path / "job"
    job_dir.mkdir()

    # setup info file
    info_file = job_dir / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    # setup runner
    runner = QQRunner()
    runner._job_dir = job_dir
    runner._work_dir = tmp_path / "work"
    runner._work_dir.mkdir()

    # sample file in work_dir and job_dir
    (runner._work_dir / "file_work.txt").write_text("work content")
    (runner._job_dir / "file_job.txt").write_text("job content")

    return runner


@pytest.mark.parametrize(
    "use_scratch,returncode",
    [
        (True, 0),
        (True, 1),
        (False, 0),
        (False, 1),
    ],
)
def test_finalize(runner_with_dirs_and_files, use_scratch, returncode):
    runner = runner_with_dirs_and_files
    runner._use_scratch = use_scratch

    class DummyProcess:
        def __init__(self, code):
            self.returncode = code

    runner._process = DummyProcess(returncode)

    runner.finalize()

    updated_info = QQInfo.fromFile(runner._job_dir / "job.qqinfo")

    if returncode == 0:
        assert updated_info.job_state == NaiveState.FINISHED
        if use_scratch:
            # all files should be in job_dir and none in work_dir
            assert (runner._job_dir / "file_job.txt").exists()
            assert (runner._job_dir / "file_work.txt").exists()
            assert not (runner._work_dir / "file_job.txt").exists()
            assert not (runner._work_dir / "file_work.txt").exists()
        else:
            # work_dir was not used at all; work file still remains there
            assert (runner._job_dir / "file_job.txt").exists()
            assert not (runner._job_dir / "file_work.txt").exists()
            assert not (runner._work_dir / "file_job.txt").exists()
            assert (runner._work_dir / "file_work.txt").exists()
    else:
        assert updated_info.job_state == NaiveState.FAILED
        if use_scratch:
            # no files copied from work_dir; all files remain on work_dir
            assert (runner._job_dir / "file_job.txt").exists()
            assert not (runner._job_dir / "file_work.txt").exists()
            assert not (runner._work_dir / "file_job.txt").exists()
            assert (runner._work_dir / "file_work.txt").exists()
        else:
            # work_dir was not used at all; work file still remains there
            assert (runner._job_dir / "file_job.txt").exists()
            assert not (runner._job_dir / "file_work.txt").exists()
            assert not (runner._work_dir / "file_job.txt").exists()
            assert (runner._work_dir / "file_work.txt").exists()


def test_set_up_shared_dir_success(runner_with_dirs):
    runner = runner_with_dirs

    # create a dummy file in the job directory
    dummy_file = runner._job_dir / "dummy.txt"
    dummy_file.write_text("hello world")

    # call the method
    runner._setUpSharedDir()

    # working directory should now be the job directory
    assert runner._work_dir == runner._job_dir
    assert Path.cwd() == runner._job_dir

    # files in job_dir should still exist
    assert (runner._work_dir / "dummy.txt").exists()


def test_set_up_scratch_dir_success(runner_with_dirs, tmp_path):
    runner = runner_with_dirs

    # create some dummy files in the job directory
    file1 = runner._job_dir / "file1.txt"
    file1.write_text("hello")
    file2 = runner._job_dir / "file2.log"
    file2.write_text("world")

    # create info file in job_dir
    runner._informer.toFile(runner._info_file)

    # mock batch system to return success
    scratch_dir = tmp_path / "scratch"
    scratch_dir.mkdir()
    runner._batch_system = MagicMock()
    runner._batch_system.getScratchDir.return_value = scratch_dir

    runner._setUpScratchDir()

    # working directory should now be scratch_dir / inner_dir
    assert runner._work_dir == scratch_dir / SCRATCH_DIR_INNER
    assert Path.cwd() == scratch_dir / SCRATCH_DIR_INNER

    # files should be copied from job_dir to work_dir
    for f in ["file1.txt", "file2.log"]:
        assert (runner._work_dir / f).exists()
        assert (runner._job_dir / f).exists()

    # info file should exist in job_dir but not in work_dir
    assert (runner._job_dir / "job.qqinfo").exists()
    assert not (runner._work_dir / "job.qqinfo").exists()


def test_set_up_scratch_dir_failure(runner_with_dirs):
    runner = runner_with_dirs

    # mock batch system to raise QQError directly
    runner._batch_system = MagicMock()

    def fail_scratch(_arg):
        raise QQError("failed to get scratch")

    runner._batch_system.getScratchDir.side_effect = fail_scratch

    with pytest.raises(QQError, match="failed to get scratch"):
        runner._setUpScratchDir()


def test_convert_absolute_to_relative_success(tmp_path):
    target = tmp_path
    file1 = target / "a.txt"
    file2 = target / "subdir" / "b.txt"
    file2.parent.mkdir()
    file1.write_text("data1")
    file2.write_text("data2")

    runner = QQRunner.__new__(QQRunner)
    result = runner._convertAbsoluteToRelative([file1, file2], target)

    assert result == [Path("a.txt"), Path("subdir") / "b.txt"]


def test_convert_absolute_to_relative_file_outside_target(tmp_path):
    target = tmp_path / "target"
    outside = tmp_path / "outside.txt"
    target.mkdir()
    outside.write_text("oops")

    runner = QQRunner.__new__(QQRunner)

    with pytest.raises(QQError, match="is not in target directory"):
        runner._convertAbsoluteToRelative([outside], target)


def test_convert_absolute_to_relative_empty_list(tmp_path):
    target = tmp_path
    runner = QQRunner.__new__(QQRunner)
    result = runner._convertAbsoluteToRelative([], target)
    assert result == []


def test_convert_absolute_to_relative_mixed_inside_and_outside(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    inside = target / "file.txt"
    inside.write_text("inside")
    outside = tmp_path / "outside.txt"
    outside.write_text("outside")

    runner = QQRunner.__new__(QQRunner)

    with pytest.raises(QQError):
        runner._convertAbsoluteToRelative([inside, outside], target)


def test_sync_directories_copies_new_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # create files in src
    (src / "file1.txt").write_text("data1")
    (src / "file2.txt").write_text("data2")

    runner = QQRunner.__new__(QQRunner)
    runner._syncDirectories(src, dest)

    # all files from src should exist in dest with same content
    for f in src.iterdir():
        dest_file = dest / f.name
        assert dest_file.exists()
        assert dest_file.read_text() == f.read_text()


def test_sync_directories_preserves_dest_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # file in dest that is not in src
    (dest / "keep.txt").write_text("keep_me")
    # file in src
    (src / "new.txt").write_text("new_data")

    runner = QQRunner.__new__(QQRunner)

    runner._syncDirectories(src, dest)

    # new file copied
    assert (dest / "new.txt").exists()
    assert (dest / "new.txt").read_text() == "new_data"
    # old file preserved
    assert (dest / "keep.txt").exists()
    assert (dest / "keep.txt").read_text() == "keep_me"
    # destination file not copied to src
    assert not (src / "keep.txt").exists()


def test_sync_directories_skips_excluded_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "include.txt").write_text("include")
    (src / "exclude.txt").write_text("exclude")

    runner = QQRunner.__new__(QQRunner)
    runner._syncDirectories(src, dest, exclude_files=[src / "exclude.txt"])

    assert (dest / "include.txt").exists()
    assert not (dest / "exclude.txt").exists()


def test_sync_directories_updates_changed_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # same file in both, dest outdated
    # note that these files have the same time of creation,
    # so they have to have different size for rsync to work properly
    (src / "file.txt").write_text("new")
    (dest / "file.txt").write_text("older")

    runner = QQRunner.__new__(QQRunner)
    runner._syncDirectories(src, dest)

    assert (dest / "file.txt").exists()
    assert (dest / "file.txt").read_text() == "new"


def test_sync_directories_rsync_failure(tmp_path, monkeypatch):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # create a file to sync
    (src / "file.txt").write_text("data")

    runner = QQRunner.__new__(QQRunner)

    # patch subprocess.run to simulate rsync failure
    def fake_run(_command, capture_output=True, text=True):
        _ = capture_output
        _ = text

        class Result:
            returncode = 1
            stderr = "rsync error"

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(QQError, match="Could not rsync files between"):
        runner._syncDirectories(src, dest)


@pytest.fixture
def runner(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()
    runner.setUp()

    Path.unlink(info_file)
    return runner


def test_delete_work_dir_some_files(runner, tmp_path):
    # set-up work dir
    runner._work_dir = tmp_path

    # create files and directories
    f1 = tmp_path / "file1.txt"
    f2 = tmp_path / "file2.txt"
    d1 = tmp_path / "dir1"
    f1.write_text("data1")
    f2.write_text("data2")
    d1.mkdir()
    (d1 / "nested.txt").write_text("nested")

    runner._deleteWorkDir()

    # assert working directory removed
    assert not tmp_path.exists()


def test_delete_work_dir_no_files(runner, tmp_path):
    # set-up work dir
    runner._work_dir = tmp_path

    # empty directory
    runner._deleteWorkDir()

    # directory should not exist
    assert not tmp_path.exists()


def test_update_info_running(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()
    runner._work_dir = Path("/scratch/job_54321.fake.server.com")
    runner._updateInfoRunning()

    updated_info = QQInfo.fromFile(info_file)
    assert updated_info.job_state == NaiveState.RUNNING
    assert updated_info.start_time is not None
    assert updated_info.main_node == socket.gethostname()
    assert updated_info.work_dir == runner._work_dir
    # unchanged
    assert updated_info.script_name == "script.sh"


def test_update_info_finished(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()
    runner._updateInfoFinished()

    updated_info = QQInfo.fromFile(info_file)
    assert updated_info.job_state == NaiveState.FINISHED
    assert updated_info.completion_time is not None
    assert updated_info.job_exit_code == 0
    # unchanged
    assert updated_info.script_name == "script.sh"


def test_update_info_failed(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()
    exit_code = 91
    runner._updateInfoFailed(exit_code)

    updated_info = QQInfo.fromFile(info_file)
    assert updated_info.job_state == NaiveState.FAILED
    assert updated_info.completion_time is not None
    assert updated_info.job_exit_code == exit_code
    # unchanged
    assert updated_info.script_name == "script.sh"


def test_update_info_killed(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()
    runner._updateInfoKilled()

    updated_info = QQInfo.fromFile(info_file)
    assert updated_info.job_state == NaiveState.KILLED
    assert updated_info.completion_time is not None
    # unchanged
    assert updated_info.script_name == "script.sh"


def test_log_failure_into_info_file(tmp_path, sample_info):
    # create a QQ info file
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()
    exit_code = 98

    # the function should update the info file and then exit
    with pytest.raises(SystemExit) as exc:
        runner.logFailureIntoInfoFile(exit_code)

    # check that the exit code is propagated
    assert exc.value.code == exit_code

    updated_info = QQInfo.fromFile(info_file)
    assert updated_info.job_state == NaiveState.FAILED
    assert updated_info.completion_time is not None
    assert updated_info.job_exit_code == exit_code
    # unchanged
    assert updated_info.script_name == "script.sh"


def test_log_failure_into_info_file_failure(tmp_path, sample_info):
    # create a QQ info file
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()

    # remove the info file to simulate write failure
    info_file.unlink()
    exit_code = 98

    # calling logFailureIntoInfoFile should still raise SystemExit
    # despite failing to update the info file
    with pytest.raises(SystemExit) as exc:
        runner.logFailureIntoInfoFile(exit_code)

    # the exit code is still propagated
    assert exc.value.code == exit_code


def test_cleanup_marks_job_killed(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()
    runner._process = subprocess.Popen(["sleep", "5"], text=True)

    runner._cleanup()

    assert runner._process.poll() is not None

    loaded_info = QQInfo.fromFile(info_file)
    assert loaded_info.job_state == NaiveState.KILLED
    assert loaded_info.completion_time is not None
    # unchanged
    assert loaded_info.script_name == "script.sh"


def test_handle_sigterm_marks_job_killed_and_exits(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()
    runner._process = subprocess.Popen(["sleep", "5"], text=True)

    with pytest.raises(SystemExit) as exc:
        runner._handle_sigterm(_signum=15, _frame=None)

    assert exc.value.code == 143  # ty: ignore[unresolved-attribute] # ty thinks SystemExit has no 'code'

    assert runner._process.poll() is not None

    loaded_info = QQInfo.fromFile(info_file)
    assert loaded_info.job_state == NaiveState.KILLED
    assert loaded_info.completion_time is not None
    # unchanged
    assert loaded_info.script_name == "script.sh"


def test_log_fatal_qq_error_logs_and_exits():
    exc = QQError("Something went wrong")
    exit_code = 42

    with patch("qq_lib.run.logger") as mock_logger, pytest.raises(SystemExit) as e:
        _log_fatal_qq_error(exc, exit_code)

    assert e.value.code == exit_code

    mock_logger.error.assert_any_call(f"Fatal qq run error: {exc}")
    mock_logger.error.assert_any_call(
        "Failure state could not be logged into the job info file. Consider the job to be in an INCONSISTENT state!"
    )


def test_log_fatal_unexpected_error_logs_and_exits():
    exc = ValueError("Unexpected problem")
    exit_code = 99

    with patch("qq_lib.run.logger") as mock_logger, pytest.raises(SystemExit) as e:
        _log_fatal_unexpected_error(exc, exit_code)

    assert e.value.code == exit_code

    mock_logger.error.assert_any_call("Fatal qq run error!")
    mock_logger.error.assert_any_call(
        "Failure state could not be logged into the job info file. Consider the job to be in an INCONSISTENT state!"
    )
    mock_logger.critical.assert_called_once_with(exc, exc_info=True, stack_info=True)
