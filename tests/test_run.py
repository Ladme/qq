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

from qq_lib.batch.interface import QQBatchMeta
from qq_lib.batch.pbs import QQPBS
from qq_lib.batch.vbs import QQVBS
from qq_lib.core.constants import (
    BATCH_SYSTEM,
    DATE_FORMAT,
    GUARD,
    INFO_FILE,
    INPUT_MACHINE,
    SCRATCH_DIR_INNER,
    SHARED_SUBMIT,
)
from qq_lib.core.error import QQError
from qq_lib.info.informer import QQInformer
from qq_lib.properties.info import QQInfo
from qq_lib.properties.job_type import QQJobType
from qq_lib.properties.loop import QQLoopInfo
from qq_lib.properties.resources import QQResources
from qq_lib.properties.states import NaiveState
from qq_lib.run import run
from qq_lib.run.runner import (
    QQRunner,
    log_fatal_qq_error,
    log_fatal_unexpected_error,
)


@pytest.fixture(autouse=True)
def autopatch_retry_wait():
    with patch("qq_lib.run.runner.RUNNER_RETRY_WAIT", 0.0):
        yield


@pytest.fixture(autouse=True)
def register():
    QQBatchMeta.register(QQPBS)
    QQBatchMeta.register(QQVBS)


@pytest.fixture(autouse=True)
def autopatch_guess():
    with patch.object(QQBatchMeta, "guess", return_value=QQPBS):
        yield


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
        stdout_file="script.out",
        stderr_file="script.err",
        resources=sample_resources,
        excluded_files=[],
        command_line=["-q", "default", "script.sh"],
        work_dir=Path("/scratch/job_12345.fake.server.com"),
    )


def write_info_file_no_scratch_and_set_env_var(tmp_path, sample_info):
    """Helper to write qqinfo file and set env var."""
    # activate qq environment
    os.environ[GUARD] = "true"
    os.environ[SHARED_SUBMIT] = "true"

    # create a job dir
    input_dir = tmp_path / "job"
    input_dir.mkdir()
    sample_info.input_dir = input_dir
    sample_info.script_name = "script.sh"

    # work in shared storage
    sample_info.resources.work_dir = "input_dir"

    info_file = input_dir / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)
    os.environ[INPUT_MACHINE] = sample_info.input_machine


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
    input_dir = tmp_path / "job"
    input_dir.mkdir()
    sample_info.input_dir = input_dir
    sample_info.script_name = "script.sh"

    # create a scratch dir & set a monkeypatch for it
    scratch_dir = tmp_path / "scratch"
    scratch_dir.mkdir()
    monkeypatch.setattr(
        QQVBS,
        "getScratchDir",
        staticmethod(lambda _: scratch_dir),
    )

    info_file = input_dir / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    os.environ[INFO_FILE] = str(info_file)
    os.environ[INPUT_MACHINE] = sample_info.input_machine


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

    with patch.object(QQBatchMeta, "guess", return_value=QQPBS):
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
    monkeypatch.delenv(GUARD)

    runner = CliRunner()
    result = runner.invoke(run, [str(tmp_path / "job" / "script.sh")])

    assert result.exit_code == 91


def test_run_scratch_nonexistent_info_file(monkeypatch, tmp_path, sample_info):
    write_info_file_with_scratch_and_set_env_var(monkeypatch, tmp_path, sample_info)
    monkeypatch.setenv(SHARED_SUBMIT, "true")

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


def test_runner_init_ok(monkeypatch, tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    write_info_file(info_file, sample_info)
    monkeypatch.setenv(INFO_FILE, str(info_file))

    runner = QQRunner()

    assert runner._informer is not None
    assert runner._info_file == info_file
    assert isinstance(runner._informer, QQInformer)


def test_runner_init_env_not_set(monkeypatch):
    monkeypatch.delenv(INFO_FILE, raising=False)

    with pytest.raises(QQError, match="not set"):
        QQRunner()


def test_runner_init_info_file_missing(monkeypatch, tmp_path):
    info_file = tmp_path / "missing.qqinfo"
    monkeypatch.setenv(INFO_FILE, str(info_file))

    with pytest.raises(QQError, match="Could not read file"):
        QQRunner()


def test_runner_init_info_file_incomplete(monkeypatch, tmp_path, sample_info):
    info_file = tmp_path / "bad.qqinfo"
    # write info file missing a single field
    informer = QQInformer(sample_info)
    # intentionally making info invalid
    informer.info.job_id = None  # ty: ignore[invalid-assignment]
    informer.toFile(info_file)

    monkeypatch.setenv(INFO_FILE, str(info_file))

    with pytest.raises(QQError, match="Mandatory information missing"):
        QQRunner()


def test_set_up_initializes_runner(monkeypatch, tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))
    monkeypatch.setenv(INPUT_MACHINE, str(socket.gethostname()))
    monkeypatch.setenv(BATCH_SYSTEM, "VBS")

    runner = QQRunner()

    runner.setUp()

    assert runner._input_dir == sample_info.input_dir
    assert runner._batch_system == sample_info.batch_system
    assert runner._use_scratch == sample_info.resources.usesScratch()


def test_set_up_initializes_runner_with_loop_info(monkeypatch, tmp_path, sample_info):
    sample_info.job_type = QQJobType.LOOP
    sample_info.loop_info = QQLoopInfo(1, 5, tmp_path / "storage", "job%03d")

    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))
    monkeypatch.setenv(INPUT_MACHINE, str(socket.gethostname()))
    monkeypatch.setenv(BATCH_SYSTEM, "VBS")

    runner = QQRunner()

    with (
        patch("qq_lib.archive.archiver.QQArchiver.makeArchiveDir") as mock_archive_dir,
        patch(
            "qq_lib.archive.archiver.QQArchiver.archiveRunTimeFiles"
        ) as mock_archive_files,
    ):
        runner.setUp()

    assert runner._input_dir == sample_info.input_dir
    assert runner._batch_system == sample_info.batch_system
    assert runner._use_scratch == sample_info.resources.usesScratch()
    assert runner._archiver is not None
    assert runner._archiver._archive == tmp_path / "storage"
    assert runner._archiver._archive_format == "job%03d"
    assert runner._archiver._input_machine == sample_info.input_machine
    assert runner._archiver._input_dir == sample_info.input_dir
    assert runner._archiver._batch_system == sample_info.batch_system

    mock_archive_dir.assert_called_once()
    mock_archive_files.assert_called_once()


@pytest.fixture
def runner_with_dirs(monkeypatch, tmp_path, sample_info):
    input_dir = tmp_path / "job"
    input_dir.mkdir()

    info_file = input_dir / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))
    monkeypatch.setenv(INPUT_MACHINE, str(socket.gethostname()))
    monkeypatch.setenv(BATCH_SYSTEM, "VBS")

    runner = QQRunner()
    runner.setUp()

    runner._work_dir = tmp_path / "work"
    runner._work_dir.mkdir()
    runner._input_dir = input_dir
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


@pytest.fixture
def loop_runner_with_dirs(monkeypatch, tmp_path, sample_info):
    input_dir = tmp_path / "job"
    input_dir.mkdir()

    sample_info.job_type = QQJobType.LOOP
    sample_info.loop_info = QQLoopInfo(1, 5, tmp_path / "storage", "job%03d", current=3)
    info_file = input_dir / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))
    monkeypatch.setenv(INPUT_MACHINE, str(socket.gethostname()))
    monkeypatch.setenv(BATCH_SYSTEM, "VBS")

    runner = QQRunner()
    with (
        patch("qq_lib.archive.archiver.QQArchiver.makeArchiveDir"),
        patch("qq_lib.archive.archiver.QQArchiver.archiveRunTimeFiles"),
    ):
        runner.setUp()

    runner._work_dir = tmp_path / "work"
    runner._work_dir.mkdir()
    runner._input_dir = input_dir
    return runner


@pytest.mark.parametrize("use_scratch", [True, False])
def test_set_up_work_dir_calls_archive(loop_runner_with_dirs, use_scratch):
    runner = loop_runner_with_dirs

    runner._use_scratch = use_scratch
    with (
        patch.object(runner, "_setUpScratchDir"),
        patch.object(runner, "_setUpSharedDir"),
        patch("qq_lib.archive.archiver.QQArchiver.fromArchive") as mock_archive,
    ):
        runner.setUpWorkDir()
        mock_archive.assert_called_once_with(
            runner._work_dir, runner._informer.info.loop_info.current
        )


def test_execute_script_success(tmp_path, runner_with_dirs):
    runner = runner_with_dirs

    success_script = tmp_path / "script.sh"
    success_script.write_text(
        "#!/usr/bin/env -S qq run\n"
        "echo 'stdout success'\n"
        "echo 'stderr success' >&2\n"
        "exit 0\n"
    )
    success_script.chmod(success_script.stat().st_mode | 0o111)

    os.chdir(tmp_path)

    ret_code = runner.executeScript()
    assert ret_code == 0

    assert "stdout success" in (tmp_path / "script.out").read_text()
    assert "stderr success" in (tmp_path / "script.err").read_text()
    updated_info = QQInfo.fromFile(runner._input_dir / "job.qqinfo")
    assert updated_info.job_state == NaiveState.RUNNING


def test_execute_script_failure(tmp_path, runner_with_dirs):
    runner = runner_with_dirs

    success_script = tmp_path / "script.sh"
    success_script.write_text(
        "#!/usr/bin/env -S qq run\n"
        "echo 'stdout success'\n"
        "echo 'stderr success' >&2\n"
        "exit 1\n"
    )
    success_script.chmod(success_script.stat().st_mode | 0o111)

    os.chdir(tmp_path)

    ret_code = runner.executeScript()
    assert ret_code == 1

    assert "stdout success" in (tmp_path / "script.out").read_text()
    assert "stderr success" in (tmp_path / "script.err").read_text()
    updated_info = QQInfo.fromFile(runner._input_dir / "job.qqinfo")
    assert updated_info.job_state == NaiveState.RUNNING


@pytest.fixture
def runner_with_dirs_and_files(monkeypatch, tmp_path, sample_info):
    input_dir = tmp_path / "job"
    input_dir.mkdir()

    # setup info file
    info_file = input_dir / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))

    # setup runner
    runner = QQRunner()
    runner._input_dir = input_dir
    runner._work_dir = tmp_path / "work"
    runner._work_dir.mkdir()

    # sample file in work_dir and input_dir
    (runner._work_dir / "file_work.txt").write_text("work content")
    (runner._input_dir / "file_job.txt").write_text("job content")

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
    runner._batch_system = QQPBS

    class DummyProcess:
        def __init__(self, code):
            self.returncode = code

    runner._process = DummyProcess(returncode)

    runner.finalize()

    updated_info = QQInfo.fromFile(runner._input_dir / "job.qqinfo")

    if returncode == 0:
        assert updated_info.job_state == NaiveState.FINISHED
        if use_scratch:
            # all files should be in input_dir and none in work_dir
            assert (runner._input_dir / "file_job.txt").exists()
            assert (runner._input_dir / "file_work.txt").exists()
            assert not (runner._work_dir / "file_job.txt").exists()
            assert not (runner._work_dir / "file_work.txt").exists()
        else:
            # work_dir was not used at all; work file still remains there
            assert (runner._input_dir / "file_job.txt").exists()
            assert not (runner._input_dir / "file_work.txt").exists()
            assert not (runner._work_dir / "file_job.txt").exists()
            assert (runner._work_dir / "file_work.txt").exists()
    else:
        assert updated_info.job_state == NaiveState.FAILED
        if use_scratch:
            # no files copied from work_dir; all files remain on work_dir
            assert (runner._input_dir / "file_job.txt").exists()
            assert not (runner._input_dir / "file_work.txt").exists()
            assert not (runner._work_dir / "file_job.txt").exists()
            assert (runner._work_dir / "file_work.txt").exists()
        else:
            # work_dir was not used at all; work file still remains there
            assert (runner._input_dir / "file_job.txt").exists()
            assert not (runner._input_dir / "file_work.txt").exists()
            assert not (runner._work_dir / "file_job.txt").exists()
            assert (runner._work_dir / "file_work.txt").exists()


@pytest.mark.parametrize("use_scratch", [True, False])
def test_finalize_calls_archive_and_resubmit(loop_runner_with_dirs, use_scratch):
    runner = loop_runner_with_dirs

    runner._use_scratch = use_scratch

    class DummyProcess:
        def __init__(self, code):
            self.returncode = code

    runner._process = DummyProcess(0)

    with (
        patch("qq_lib.archive.archiver.QQArchiver.toArchive") as mock_archive,
        patch.object(QQRunner, "_resubmit") as mock_resubmit,
    ):
        runner.finalize()
        mock_archive.assert_called_once_with(runner._work_dir)
        mock_resubmit.assert_called_once()


def test_set_up_shared_dir_success(runner_with_dirs):
    runner = runner_with_dirs

    # create a dummy file in the job directory
    dummy_file = runner._input_dir / "dummy.txt"
    dummy_file.write_text("hello world")

    # call the method
    runner._setUpSharedDir()

    # working directory should now be the job directory
    assert runner._work_dir == runner._input_dir
    assert Path.cwd() == runner._input_dir

    # files in input_dir should still exist
    assert (runner._work_dir / "dummy.txt").exists()


def test_set_up_scratch_dir_success(runner_with_dirs, tmp_path):
    runner = runner_with_dirs

    # create some dummy files in the job directory
    file1 = runner._input_dir / "file1.txt"
    file1.write_text("hello")
    file2 = runner._input_dir / "file2.log"
    file2.write_text("world")

    # create info file in input_dir
    runner._informer.toFile(runner._info_file)

    # mock batch system to return success
    scratch_dir = tmp_path / "scratch"
    scratch_dir.mkdir()

    with patch.object(QQVBS, "getScratchDir", return_value=scratch_dir):
        runner._setUpScratchDir()

    # working directory should now be scratch_dir / inner_dir
    assert runner._work_dir == scratch_dir / SCRATCH_DIR_INNER
    assert Path.cwd() == scratch_dir / SCRATCH_DIR_INNER

    # files should be copied from input_dir to work_dir
    for f in ["file1.txt", "file2.log"]:
        assert (runner._work_dir / f).exists()
        assert (runner._input_dir / f).exists()

    # info file should exist in input_dir but not in work_dir
    assert (runner._input_dir / "job.qqinfo").exists()
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


def test_set_up_scratch_dir_excluded_files_with_archive(
    loop_runner_with_dirs, tmp_path
):
    runner = loop_runner_with_dirs

    # create some dummy files in the job directory
    file1 = runner._input_dir / "file1.txt"
    file1.write_text("hello")
    file2 = runner._input_dir / "file2.log"
    file2.write_text("world")

    # change the archive path to one of the created files
    runner._archiver._archive = file2

    # create info file in input_dir
    runner._informer.toFile(runner._info_file)

    # mock batch system to return success
    scratch_dir = tmp_path / "scratch"
    scratch_dir.mkdir()

    with patch.object(QQVBS, "getScratchDir", return_value=scratch_dir):
        runner._setUpScratchDir()

    # working directory should now be scratch_dir / inner_dir
    assert runner._work_dir == scratch_dir / SCRATCH_DIR_INNER
    assert Path.cwd() == scratch_dir / SCRATCH_DIR_INNER

    # file1 should be copied from input_dir to work_dir
    assert (runner._work_dir / "file1.txt").exists()
    assert (runner._input_dir / "file1.txt").exists()

    # but file2 should only exist in input_dir (it is the 'archive')
    assert (runner._input_dir / "file2.log").exists()
    assert not (runner._work_dir / "file2.log").exists()

    # info file should exist in input_dir but not in work_dir
    assert (runner._input_dir / "job.qqinfo").exists()
    assert not (runner._work_dir / "job.qqinfo").exists()


@pytest.fixture
def runner(monkeypatch, tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))

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


def test_update_info_running(monkeypatch, tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))

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


def test_update_info_finished(monkeypatch, tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))

    runner = QQRunner()
    runner._updateInfoFinished()

    updated_info = QQInfo.fromFile(info_file)
    assert updated_info.job_state == NaiveState.FINISHED
    assert updated_info.completion_time is not None
    assert updated_info.job_exit_code == 0
    # unchanged
    assert updated_info.script_name == "script.sh"


def test_update_info_failed(monkeypatch, tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))

    runner = QQRunner()
    exit_code = 91
    runner._updateInfoFailed(exit_code)

    updated_info = QQInfo.fromFile(info_file)
    assert updated_info.job_state == NaiveState.FAILED
    assert updated_info.completion_time is not None
    assert updated_info.job_exit_code == exit_code
    # unchanged
    assert updated_info.script_name == "script.sh"


def test_update_info_killed(monkeypatch, tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))

    runner = QQRunner()
    runner._updateInfoKilled()

    updated_info = QQInfo.fromFile(info_file)
    assert updated_info.job_state == NaiveState.KILLED
    assert updated_info.completion_time is not None
    # unchanged
    assert updated_info.script_name == "script.sh"


def test_log_failure_into_info_file(monkeypatch, tmp_path, sample_info):
    # create a QQ info file
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))

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


def test_log_failure_into_info_file_failure(monkeypatch, tmp_path, sample_info):
    # create a QQ info file
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))

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


def test_cleanup_marks_job_killed(monkeypatch, tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))

    runner = QQRunner()
    runner._process = subprocess.Popen(["sleep", "5"], text=True)

    runner._cleanup()

    assert runner._process.poll() is not None

    loaded_info = QQInfo.fromFile(info_file)
    assert loaded_info.job_state == NaiveState.KILLED
    assert loaded_info.completion_time is not None
    # unchanged
    assert loaded_info.script_name == "script.sh"


def test_handle_sigterm_marks_job_killed_and_exits(monkeypatch, tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.toFile(info_file)
    monkeypatch.setenv(INFO_FILE, str(info_file))

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

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        pytest.raises(SystemExit) as e,
    ):
        log_fatal_qq_error(exc, exit_code)

    assert e.value.code == exit_code

    mock_logger.error.assert_any_call(f"Fatal qq run error: {exc}")
    mock_logger.error.assert_any_call(
        "Failure state could not be logged into the job info file. Consider the job to be in an INCONSISTENT state!"
    )


def test_log_fatal_unexpected_error_logs_and_exits():
    exc = ValueError("Unexpected problem")
    exit_code = 99

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        pytest.raises(SystemExit) as e,
    ):
        log_fatal_unexpected_error(exc, exit_code)

    assert e.value.code == exit_code

    mock_logger.error.assert_any_call("Fatal qq run error!")
    mock_logger.error.assert_any_call(
        "Failure state could not be logged into the job info file. Consider the job to be in an INCONSISTENT state!"
    )
    mock_logger.critical.assert_called_once_with(exc, exc_info=True, stack_info=True)


def test_reload_info_running(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    sample_info.toFile(info_file)

    runner = object.__new__(QQRunner)
    runner._info_file = info_file
    runner._input_machine = None

    runner._reloadInfoAndCheckKill()
    assert runner._informer.info.job_state == NaiveState.RUNNING


def test_reload_info_killed_exits(tmp_path, sample_info):
    info_file = tmp_path / "job.qqinfo"
    informer = QQInformer(sample_info)
    informer.setKilled(datetime.now())
    informer.toFile(info_file)

    runner = object.__new__(QQRunner)
    runner._info_file = info_file
    runner._input_machine = None

    with pytest.raises(SystemExit):
        runner._reloadInfoAndCheckKill()


def make_runner_with_command_line(sample_info, command_line):
    """Helper to construct a QQRunner with a given command line."""
    info = sample_info
    info.command_line = command_line
    runner = QQRunner.__new__(QQRunner)
    runner._informer = QQInformer.__new__(QQInformer)
    runner._informer.info = info
    return runner


def test_prepare_command_line_no_depend(sample_info):
    cmd = ["script.sh", "-q", "gpu"]
    runner = make_runner_with_command_line(sample_info, cmd)

    result = runner._prepareCommandLineForResubmit()

    assert result == cmd + [f"--depend=afterok={sample_info.job_id}"]


def test_prepare_command_line_inline_depend(sample_info):
    cmd = ["script.sh", "--depend=afterok=11111", "-q", "gpu"]
    runner = make_runner_with_command_line(sample_info, cmd)

    result = runner._prepareCommandLineForResubmit()

    assert "--depend=afterok=11111" not in result
    assert result == [
        "script.sh",
        "-q",
        "gpu",
        f"--depend=afterok={sample_info.job_id}",
    ]


def test_prepare_command_line_separate_depend_argument(sample_info):
    cmd = ["script.sh", "--depend", "afterok=11111", "-q", "gpu"]
    runner = make_runner_with_command_line(sample_info, cmd)

    result = runner._prepareCommandLineForResubmit()

    assert "--depend" not in result
    assert "afterok=11111" not in result
    assert result == [
        "script.sh",
        "-q",
        "gpu",
        f"--depend=afterok={sample_info.job_id}",
    ]


def test_prepare_command_line_multiple_depends(sample_info):
    cmd = [
        "script.sh",
        "--depend=afterok=11111",
        "--depend",
        "afterany=33333",
        "--depend=after=22222",
        "-q",
        "gpu",
    ]
    runner = make_runner_with_command_line(sample_info, cmd)

    result = runner._prepareCommandLineForResubmit()

    assert "--depend" not in result
    assert all("afterok=11111" not in arg for arg in result)
    assert all("afterany=33333" not in arg for arg in result)

    # only one new depend at the end
    assert result[-1] == f"--depend=afterok={sample_info.job_id}"
    assert "gpu" in result
    assert "script.sh" in result


def test_prepare_command_line_depend_last_arg(sample_info):
    cmd = ["script.sh", "--depend"]
    runner = make_runner_with_command_line(sample_info, cmd)

    result = runner._prepareCommandLineForResubmit()

    # '--depend' should be removed even without a following argument
    assert "--depend" not in result
    assert result == [
        "script.sh",
        f"--depend=afterok={sample_info.job_id}",
    ]


def test_prepare_command_line_only_depend(sample_info):
    cmd = ["--depend=afterok=11111"]
    runner = make_runner_with_command_line(sample_info, cmd)

    result = runner._prepareCommandLineForResubmit()

    assert result == [f"--depend=afterok={sample_info.job_id}"]


def test_runner_resubmit_does_not_resubmit_when_final_cycle(loop_runner_with_dirs):
    runner = loop_runner_with_dirs
    runner._informer.info.loop_info.current = runner._informer.info.loop_info.end

    with patch("qq_lib.core.retryer.QQRetryer") as mock_retryer:
        runner._resubmit()

    mock_retryer.assert_not_called()


def test_runner_resubmit_invokes_retryer_with_correct_arguments(loop_runner_with_dirs):
    runner = loop_runner_with_dirs

    with (
        patch("qq_lib.run.runner.QQRetryer") as mock_retryer_class,
    ):
        mock_retryer_instance = MagicMock()
        mock_retryer_class.return_value = mock_retryer_instance

        runner._resubmit()

    mock_retryer_class.assert_called_once()
    mock_retryer_instance.run.assert_called_once()

    args, kwargs = mock_retryer_class.call_args
    assert args[0] == runner._batch_system.resubmit
    assert kwargs["input_machine"] == runner._informer.info.input_machine
    assert kwargs["input_dir"] == runner._informer.info.input_dir
    assert kwargs["command_line"] == [
        "-q",
        "default",
        "script.sh",
        "--depend=afterok=12345.fake.server.com",
    ]
    assert "max_tries" in kwargs
    assert "wait_seconds" in kwargs


def test_runner_resubmit_raises_qqerror_on_failure(loop_runner_with_dirs):
    runner = loop_runner_with_dirs

    with (
        patch("qq_lib.run.runner.QQRetryer") as mock_retryer_class,
    ):
        mock_retryer_instance = MagicMock()
        mock_retryer_instance.run.side_effect = QQError("Resubmit failed")
        mock_retryer_class.return_value = mock_retryer_instance

        with pytest.raises(QQError, match="Resubmit failed"):
            runner._resubmit()
