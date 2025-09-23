# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime
import os
from pathlib import Path
import socket
import subprocess
import pytest
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from qq_lib.batch import BatchOperationResult
from qq_lib.constants import DATE_FORMAT, GUARD, INFO_FILE
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

def write_info_file_no_scratch_and_set_env_var(monkeypatch, tmp_path, sample_info):
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

def test_run_no_scratch_finishes(monkeypatch, tmp_path, sample_info):
    write_info_file_with_scratch_and_set_env_var(monkeypatch, tmp_path, sample_info)

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
    monkeypatch.setattr(QQVBS, "getScratchDir", staticmethod(lambda job_id: BatchOperationResult.success(str(scratch_dir))))

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

def test_run_scratch_fails(monkeypatch, tmp_path, sample_info):
    write_info_file_with_scratch_and_set_env_var(monkeypatch, tmp_path, sample_info)

    script = tmp_path / "job" / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\necho hello\nexit 2\n")

    runner = CliRunner()
    result = runner.invoke(run, [str(tmp_path / "job" / "script.sh")])

    assert result.exit_code == 2
    # should not be copied from work directory
    assert "hello" in (tmp_path / "scratch" / "script.out").read_text()

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
    informer.info.job_id = None
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
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    runner = QQRunner()
    runner.setUp()

    Path.unlink(info_file)

    runner._work_dir = tmp_path / "work"
    runner._work_dir.mkdir()
    runner._job_dir = tmp_path / "job"
    runner._job_dir.mkdir()
    return runner

def test_set_up_work_dir_calls_correct_method(runner_with_dirs):
    runner = runner_with_dirs

    # use scratch
    runner._use_scratch = True
    with patch.object(runner, "_setUpScratchDir") as mock_scratch, \
         patch.object(runner, "_setUpSharedDir") as mock_shared:
        runner.setUpWorkDir()
        mock_scratch.assert_called_once()
        mock_shared.assert_not_called()

    # no scratch
    runner._use_scratch = False
    with patch.object(runner, "_setUpScratchDir") as mock_scratch, \
         patch.object(runner, "_setUpSharedDir") as mock_shared:
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
    updated_info = QQInfo.fromFile(tmp_path / "job.qqinfo")
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
    updated_info = QQInfo.fromFile(tmp_path / "job.qqinfo")
    assert updated_info.job_state == NaiveState.RUNNING

@pytest.fixture
def runner_with_dirs_and_files(tmp_path, sample_info):
    # setup info file
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)

    # setup runner
    runner = QQRunner()
    runner._job_dir = tmp_path / "job"
    runner._work_dir = tmp_path / "work"
    runner._job_dir.mkdir()
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
    ]
)
def test_complete(tmp_path, runner_with_dirs_and_files, use_scratch, returncode):
    runner = runner_with_dirs_and_files
    runner._use_scratch = use_scratch

    class DummyProcess:
        def __init__(self, code):
            self.returncode = code

    runner._process = DummyProcess(returncode)

    runner.complete()

    updated_info = QQInfo.fromFile(tmp_path / "job.qqinfo")

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

def test_setUpSharedDir(runner_with_dirs):
    runner = runner_with_dirs

    # create a dummy file in the job directory
    dummy_file = runner._job_dir / "dummy.txt"
    dummy_file.write_text("hello world")

    # call the method
    runner._setUpSharedDir()

    # working directory should now be the job directory
    assert runner._work_dir == runner._job_dir
    assert os.getcwd() == str(runner._job_dir)

    # files in job_dir should still exist
    assert (runner._work_dir / "dummy.txt").exists()


def test_set_up_scratch_dir_success(runner_with_dirs, tmp_path):
    runner = runner_with_dirs

    # create some dummy files in the job directory
    file1 = runner._job_dir / "file1.txt"
    file1.write_text("hello")
    file2 = runner._job_dir / "file2.log"
    file2.write_text("world")

    # mock batch system to return success
    scratch_dir = tmp_path / "scratch"
    scratch_dir.mkdir()
    runner._batch_system = MagicMock()
    runner._batch_system.getScratchDir.return_value = BatchOperationResult.success(str(scratch_dir))

    runner._setUpScratchDir()

    # working directory should now be scratch_dir
    assert runner._work_dir == scratch_dir
    assert os.getcwd() == str(scratch_dir)

    # files should be copied from job_dir to work_dir
    for f in ["file1.txt", "file2.log"]:
        assert (runner._work_dir / f).exists()
        assert (runner._job_dir / f).exists()

def test_set_up_scratch_dir_failure(runner_with_dirs):
    runner = runner_with_dirs

    # mock batch system to fail
    runner._batch_system = MagicMock()
    runner._batch_system.getScratchDir.return_value = BatchOperationResult.error(1, "failed to get scratch")

    with pytest.raises(QQError, match="failed to get scratch"):
        runner._setUpScratchDir()

def test_copy_files_from_workdir_some_files(runner_with_dirs):
    # create files and directories in work_dir
    f1 = runner_with_dirs._work_dir / "file1.txt"
    f2 = runner_with_dirs._work_dir / "file2.txt"
    d1 = runner_with_dirs._work_dir / "dir1"
    f1.write_text("data1")
    f2.write_text("data2")
    d1.mkdir()
    (d1 / "nested.txt").write_text("nested")

    runner_with_dirs._copyFilesFromWorkDir()

    for path in runner_with_dirs._work_dir.iterdir():
        target_path = runner_with_dirs._job_dir / path.name
        assert target_path.exists()
        if path.is_file():
            # files content must match
            assert target_path.read_text() == path.read_text()
        elif path.is_dir():
            # directories and nested files must exist
            nested_file = target_path / "nested.txt"
            assert nested_file.exists()
            assert nested_file.read_text() == "nested"

def test_copy_files_from_workdir_no_files(runner_with_dirs):
    # nothing in work_dir
    runner_with_dirs._copyFilesFromWorkDir()
    assert list(runner_with_dirs._job_dir.iterdir()) == []

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

def test_copy_files_to_dst(runner, tmp_path):
    # create job directory
    dest = tmp_path / "job"
    dest.mkdir()

    # create files and directories
    f1 = tmp_path / "file1.txt"
    f2 = tmp_path / "file2.txt"
    f3 = tmp_path / "file3.txt" # should not be copied
    d1 = tmp_path / "dir1"
    f1.write_text("content1")
    f2.write_text("content2")
    d1.mkdir()
    (d1 / "nested.txt").write_text("nested")

    # list of paths to copy
    src_paths = [f1, f2, d1]

    runner._copyFilesToDst(src_paths, dest)

    for path in src_paths:
        dest_path = dest / path.name
        assert dest_path.exists()
        if path.is_file():
            assert dest_path.read_text() == path.read_text()
        elif path.is_dir():
            nested_file = dest_path / "nested.txt"
            assert nested_file.exists()
            assert nested_file.read_text() == "nested"
    
    # not copied
    assert not (dest / f3.name).exists()
    

def test_remove_files_from_workdir_some_files(runner, tmp_path):
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

    runner._removeFilesFromWorkDir()

    # assert everything removed
    assert not any(tmp_path.iterdir())

def test_remove_files_from_workdir_no_files(runner, tmp_path):
    # set-up work dir
    runner._work_dir = tmp_path

    # empty directory
    runner._removeFilesFromWorkDir()

    # nothing should break and directory remains empty
    assert list(tmp_path.iterdir()) == []

def test_get_files_to_copy_some_files_no_excluded(tmp_path, runner):
    # create some files
    (tmp_path / "file1.txt").write_text("hello")
    (tmp_path / "file2.txt").write_text("world")
    # directory
    d1 = tmp_path / "dir1"
    d1.mkdir()
    (d1 / "nested.txt").write_text("nested")
    
    result = runner._getFilesToCopy(tmp_path)
    assert set(result) == {tmp_path / "file1.txt", tmp_path / "file2.txt", tmp_path / "dir1"}

def test_get_files_to_copy_no_files_excluded(tmp_path, runner):
    # create some files
    (tmp_path / "file1.txt").write_text("hello")
    (tmp_path / "file2.txt").write_text("world")
    
    # exclude both files
    excluded = [tmp_path / "file1.txt", tmp_path / "file2.txt"]
    result = runner._getFilesToCopy(tmp_path, filter_out=excluded)
    assert result == []

def test_get_files_to_copy_some_files_some_excluded(tmp_path, runner):
    # create files
    (tmp_path / "file1.txt").write_text("hello")
    (tmp_path / "file2.txt").write_text("world")
    (tmp_path / "file3.txt").write_text("!")
    # directory
    d1 = tmp_path / "dir1"
    d1.mkdir()
    (d1 / "nested.txt").write_text("nested")
    
    # exclude file2 and dir1
    excluded = [tmp_path / "file2.txt", tmp_path / "dir1"]
    result = runner._getFilesToCopy(tmp_path, filter_out=excluded)
    assert set(result) == {tmp_path / "file1.txt", tmp_path / "file3.txt"}


def test_get_files_to_copy_no_files_no_excluded(tmp_path, runner):
    # empty directory
    result = runner._getFilesToCopy(tmp_path)
    assert result == []

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
    runner._process = subprocess.Popen(["sleep", "5"])

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
    runner._process = subprocess.Popen(["sleep", "5"])

    with pytest.raises(SystemExit) as exc:
        runner._handle_sigterm(_signum=15, _frame=None)
    
    assert exc.value.code == 143

    assert runner._process.poll() is not None
    
    loaded_info = QQInfo.fromFile(info_file)
    assert loaded_info.job_state == NaiveState.KILLED
    assert loaded_info.completion_time is not None
    # unchanged
    assert loaded_info.script_name == "script.sh"

def test_log_fatal_qq_error_logs_and_exits():
    exc = QQError("Something went wrong")
    exit_code = 42

    with patch("qq_lib.run.logger") as mock_logger:
        with pytest.raises(SystemExit) as e:
            _log_fatal_qq_error(exc, exit_code)

    assert e.value.code == exit_code

    mock_logger.error.assert_any_call(f"Fatal qq run error: {exc}")
    mock_logger.error.assert_any_call(
        "Failure state could not be logged into the job info file. Consider the job to be in an INCONSISTENT state!"
    )

def test_log_fatal_unexpected_error_logs_and_exits():
    exc = ValueError("Unexpected problem")
    exit_code = 99

    with patch("qq_lib.run.logger") as mock_logger:
        with pytest.raises(SystemExit) as e:
            _log_fatal_unexpected_error(exc, exit_code)

    assert e.value.code == exit_code

    mock_logger.error.assert_any_call("Fatal qq run error!")
    mock_logger.error.assert_any_call(
        "Failure state could not be logged into the job info file. Consider the job to be in an INCONSISTENT state!"
    )
    mock_logger.critical.assert_called_once_with(exc, exc_info=True, stack_info=True)