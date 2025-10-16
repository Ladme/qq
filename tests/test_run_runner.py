# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import shutil
import signal
import subprocess
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.constants import (
    RUNNER_RETRY_TRIES,
    RUNNER_RETRY_WAIT,
    RUNNER_SIGTERM_TO_SIGKILL,
    SCRATCH_DIR_INNER,
    UNEXPECTED_EXCEPTION_EXIT_CODE,
)
from qq_lib.core.error import QQError, QQRunCommunicationError, QQRunFatalError
from qq_lib.info.informer import QQInformer
from qq_lib.properties.job_type import QQJobType
from qq_lib.properties.states import NaiveState
from qq_lib.run.runner import QQRunner, log_fatal_error_and_exit


def test_qqrunner_init_without_loop_info():
    informer_mock = MagicMock()
    informer_mock.info.job_id = "123"
    informer_mock.info.input_dir = "/input"
    informer_mock.batch_system = MagicMock()
    informer_mock.usesScratch.return_value = True
    informer_mock.info.loop_info = None

    retryer_mock = MagicMock()
    retryer_mock.run.return_value = informer_mock

    with (
        patch("qq_lib.run.runner.signal.signal") as signal_mock,
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock),
        patch("qq_lib.run.runner.QQInformer.fromFile"),
        patch("qq_lib.run.runner.logger"),
        patch("qq_lib.run.runner.socket.gethostname", return_value="localhost"),
    ):
        runner = QQRunner(Path("job.qqinfo"), "host")

    signal_mock.assert_called_once_with(signal.SIGTERM, runner._handle_sigterm)
    assert isinstance(runner._process, type(None))
    assert runner._info_file == Path("job.qqinfo")
    assert runner._input_machine == "host"
    assert runner._informer == informer_mock
    assert runner._input_dir == Path("/input")
    assert runner._batch_system == informer_mock.batch_system
    assert runner._use_scratch is True
    assert runner._archiver is None


def test_qqrunner_init_with_loop_info():
    loop_info_mock = MagicMock()
    loop_info_mock.archive = "storage"
    loop_info_mock.archive_format = "job%03d"
    loop_info_mock.current = 3

    informer_mock = MagicMock()
    informer_mock.info.job_id = "123"
    informer_mock.info.input_dir = "/input"
    informer_mock.info.input_machine = "host"
    informer_mock.info.script_name = "job.sh"
    informer_mock.info.loop_info = loop_info_mock
    informer_mock.batch_system = MagicMock()
    informer_mock.usesScratch.return_value = False

    archiver_mock = MagicMock()
    retryer_mock = MagicMock()
    retryer_mock.run.return_value = informer_mock

    with (
        patch("qq_lib.run.runner.signal.signal"),
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock),
        patch("qq_lib.run.runner.QQInformer.fromFile"),
        patch("qq_lib.run.runner.QQArchiver", return_value=archiver_mock),
        patch("qq_lib.run.runner.logger"),
        patch("qq_lib.run.runner.socket.gethostname", return_value="localhost"),
    ):
        runner = QQRunner(Path("job.qqinfo"), "host")

    assert runner._archiver == archiver_mock
    archiver_mock.makeArchiveDir.assert_called_once()
    archiver_mock.archiveRunTimeFiles.assert_called_once_with("job.sh\\+0002", 2)


def test_qqrunner_init_raises_on_load_failure():
    retryer_mock = MagicMock()
    retryer_mock.run.side_effect = Exception("fatal")

    with (
        patch("qq_lib.run.runner.signal.signal"),
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock),
        patch("qq_lib.run.runner.QQInformer.fromFile"),
        pytest.raises(QQRunFatalError, match="Unable to load qq info file"),
    ):
        QQRunner(Path("job.qqinfo"), "host")


def test_qqrunner_handle_sigterm_performs_cleanup_and_exits():
    runner = QQRunner.__new__(QQRunner)
    runner._cleanup = MagicMock()

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.sys.exit", side_effect=SystemExit(143)) as mock_exit,
        pytest.raises(SystemExit) as exc_info,
    ):
        runner._handle_sigterm(signal.SIGTERM, None)

    mock_logger.info.assert_called_once_with("Received SIGTERM, initiating shutdown.")
    runner._cleanup.assert_called_once()
    mock_logger.error.assert_called_once_with("Execution was terminated by SIGTERM.")
    mock_exit.assert_called_once_with(143)
    assert exc_info.value.code == 143  # ty: ignore[unresolved-attribute]


def test_qqrunner_cleanup_with_running_process():
    runner = QQRunner.__new__(QQRunner)
    runner._updateInfoKilled = MagicMock()
    process_mock = MagicMock()
    process_mock.poll.return_value = None
    runner._process = process_mock

    with patch("qq_lib.run.runner.logger") as mock_logger:
        runner._cleanup()

    runner._updateInfoKilled.assert_called_once()
    mock_logger.info.assert_called_once_with("Cleaning up: terminating subprocess.")
    process_mock.terminate.assert_called_once()
    process_mock.wait.assert_called_once_with(timeout=RUNNER_SIGTERM_TO_SIGKILL)
    process_mock.kill.assert_not_called()


def test_qqrunner_cleanup_with_timeout():
    runner = QQRunner.__new__(QQRunner)
    runner._updateInfoKilled = MagicMock()
    process_mock = MagicMock()
    process_mock.poll.return_value = None
    process_mock.wait.side_effect = subprocess.TimeoutExpired(cmd="cmd", timeout=1)
    runner._process = process_mock

    with patch("qq_lib.run.runner.logger") as mock_logger:
        runner._cleanup()

    runner._updateInfoKilled.assert_called_once()
    mock_logger.info.assert_any_call("Cleaning up: terminating subprocess.")
    mock_logger.info.assert_any_call("Subprocess did not exit, killing.")
    process_mock.terminate.assert_called_once()
    process_mock.kill.assert_called_once()


def test_qqrunner_cleanup_without_running_process():
    runner = QQRunner.__new__(QQRunner)
    runner._updateInfoKilled = MagicMock()
    process_mock = MagicMock()
    process_mock.poll.return_value = 0
    runner._process = process_mock

    with patch("qq_lib.run.runner.logger"):
        runner._cleanup()

    runner._updateInfoKilled.assert_called_once()
    process_mock.terminate.assert_not_called()
    process_mock.kill.assert_not_called()


def test_qqrunner_prepare_command_line_for_resubmit_inline_depend():
    informer_mock = MagicMock()
    informer_mock.info.command_line = [
        "script.sh",
        "--depend=afterok=11111",
        "-q",
        "gpu",
    ]
    informer_mock.info.job_id = "99999"
    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock

    result = runner._prepareCommandLineForResubmit()

    assert "--depend=afterok=11111" not in result
    assert result == ["script.sh", "-q", "gpu", "--depend=afterok=99999"]


def test_qqrunner_prepare_command_line_for_resubmit_separate_depend_argument():
    informer_mock = MagicMock()
    informer_mock.info.command_line = [
        "script.sh",
        "--depend",
        "afterok=11111",
        "-q",
        "gpu",
    ]
    informer_mock.info.job_id = "99999"
    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock

    result = runner._prepareCommandLineForResubmit()

    assert "--depend" not in result
    assert "afterok=11111" not in result
    assert result == ["script.sh", "-q", "gpu", "--depend=afterok=99999"]


def test_qqrunner_prepare_command_line_for_resubmit_multiple_depends():
    informer_mock = MagicMock()
    informer_mock.info.command_line = [
        "script.sh",
        "--depend=afterok=11111",
        "--depend",
        "afterany=33333",
        "--depend=after=22222",
        "-q",
        "gpu",
    ]
    informer_mock.info.job_id = "99999"
    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock

    result = runner._prepareCommandLineForResubmit()

    assert "--depend" not in result
    assert all("afterok=11111" not in arg for arg in result)
    assert all("afterany=33333" not in arg for arg in result)
    assert result[-1] == "--depend=afterok=99999"
    assert "gpu" in result
    assert "script.sh" in result


def test_qqrunner_prepare_command_line_for_resubmit_depend_last_arg():
    informer_mock = MagicMock()
    informer_mock.info.command_line = ["script.sh", "--depend"]
    informer_mock.info.job_id = "99999"
    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock

    result = runner._prepareCommandLineForResubmit()

    assert "--depend" not in result
    assert result == ["script.sh", "--depend=afterok=99999"]


def test_qqrunner_prepare_command_line_for_resubmit_only_depend():
    informer_mock = MagicMock()
    informer_mock.info.command_line = ["--depend=afterok=11111"]
    informer_mock.info.job_id = "99999"
    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock

    result = runner._prepareCommandLineForResubmit()

    assert result == ["--depend=afterok=99999"]


def test_qqrunner_resubmit_final_cycle():
    informer_mock = MagicMock()
    informer_mock.info.loop_info.current = 5
    informer_mock.info.loop_info.end = 5

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock

    with patch("qq_lib.run.runner.logger") as mock_logger:
        runner._resubmit()

    mock_logger.info.assert_called_once_with(
        "This was the final cycle of the loop job. Not resubmitting."
    )


def test_qqrunner_resubmit_successful_resubmission():
    informer_mock = MagicMock()
    informer_mock.info.loop_info.current = 1
    informer_mock.info.loop_info.end = 5
    informer_mock.info.input_machine = "random.host.org"
    informer_mock.info.input_dir = "/dir"
    informer_mock.info.job_id = "123"
    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._batch_system = MagicMock()
    runner._prepareCommandLineForResubmit = MagicMock(return_value=["cmd"])

    retryer_mock = MagicMock()
    retryer_mock.run.return_value = None

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock) as mock_retryer,
    ):
        runner._resubmit()

    mock_logger.info.assert_any_call("Resubmitting the job.")
    mock_retryer.assert_called_once_with(
        runner._batch_system.resubmit,
        input_machine="random.host.org",
        input_dir="/dir",
        command_line=["cmd"],
        max_tries=RUNNER_RETRY_TRIES,
        wait_seconds=RUNNER_RETRY_WAIT,
    )
    retryer_mock.run.assert_called_once()
    mock_logger.info.assert_any_call("Job successfully resubmitted.")


def test_qqrunner_resubmit_raises_qqerror():
    informer_mock = MagicMock()
    informer_mock.info.loop_info.current = 1
    informer_mock.info.loop_info.end = 5
    informer_mock.info.input_machine = "random.host.org"
    informer_mock.info.input_dir = "/dir"
    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._batch_system = MagicMock()
    runner._prepareCommandLineForResubmit = MagicMock(return_value=["cmd"])

    with (
        patch("qq_lib.run.runner.QQRetryer", side_effect=QQError("resubmit failed")),
        patch("qq_lib.run.runner.logger"),
        pytest.raises(QQError, match="resubmit failed"),
    ):
        runner._resubmit()


def test_qqrunner_reload_info_and_ensure_not_killed_success():
    informer_mock = MagicMock()
    informer_mock.info.job_state = NaiveState.RUNNING

    retryer_mock = MagicMock()
    retryer_mock.run.return_value = informer_mock

    runner = QQRunner.__new__(QQRunner)
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"

    with patch(
        "qq_lib.run.runner.QQRetryer", return_value=retryer_mock
    ) as retryer_class:
        runner._reloadInfoAndEnsureNotKilled()

    retryer_class.assert_called_once_with(
        QQInformer.fromFile,
        runner._info_file,
        host=runner._input_machine,
        max_tries=RUNNER_RETRY_TRIES,
        wait_seconds=RUNNER_RETRY_WAIT,
    )
    retryer_mock.run.assert_called_once()
    assert runner._informer == informer_mock


def test_qqrunner_reload_info_and_ensure_not_killed_raises_if_killed():
    informer_mock = MagicMock()
    informer_mock.info.job_state = NaiveState.KILLED

    retryer_mock = MagicMock()
    retryer_mock.run.return_value = informer_mock

    runner = QQRunner.__new__(QQRunner)
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"

    with (
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock),
        pytest.raises(QQRunCommunicationError, match="Job has been killed"),
    ):
        runner._reloadInfoAndEnsureNotKilled()


def test_qqrunner_reload_info_and_ensure_not_killed_raises_if_retryer_fails():
    retryer_mock = MagicMock()
    retryer_mock.run.side_effect = QQError("file read failed")

    runner = QQRunner.__new__(QQRunner)
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"

    with (
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock),
        pytest.raises(QQError, match="file read failed"),
    ):
        runner._reloadInfoAndEnsureNotKilled()


def test_qqrunner_update_info_killed_success():
    informer_mock = MagicMock()
    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.datetime") as datetime_mock,
    ):
        now = datetime(2024, 1, 1)
        datetime_mock.now.return_value = now

        runner._updateInfoKilled()

    informer_mock.setKilled.assert_called_once_with(now)
    informer_mock.toFile.assert_called_once_with(
        runner._info_file, host="random.host.org"
    )
    mock_logger.warning.assert_not_called()


def test_qqrunner_update_info_killed_logs_warning_on_failure():
    informer_mock = MagicMock()
    informer_mock.setKilled.side_effect = Exception("fail")

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"

    with patch("qq_lib.run.runner.logger") as mock_logger:
        runner._updateInfoKilled()

    informer_mock.setKilled.assert_called_once()
    mock_logger.warning.assert_called_once()


def test_qqrunner_update_info_failed_success():
    informer_mock = MagicMock()
    retryer_mock = MagicMock()
    retryer_mock.run.return_value = None

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"
    runner._reloadInfoAndEnsureNotKilled = MagicMock()

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.datetime") as datetime_mock,
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock) as retryer_cls,
    ):
        now = datetime(2024, 1, 1)
        datetime_mock.now.return_value = now

        runner._updateInfoFailed(42)

    runner._reloadInfoAndEnsureNotKilled.assert_called_once()
    informer_mock.setFailed.assert_called_once_with(now, 42)
    retryer_cls.assert_called_once_with(
        informer_mock.toFile,
        runner._info_file,
        host="random.host.org",
        max_tries=RUNNER_RETRY_TRIES,
        wait_seconds=RUNNER_RETRY_WAIT,
    )
    retryer_mock.run.assert_called_once()
    mock_logger.warning.assert_not_called()


def test_qqrunner_update_info_failed_logs_warning_on_failure():
    informer_mock = MagicMock()
    informer_mock.setFailed.side_effect = Exception("fail")

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"
    runner._reloadInfoAndEnsureNotKilled = MagicMock()

    with patch("qq_lib.run.runner.logger") as mock_logger:
        runner._updateInfoFailed(99)

    runner._reloadInfoAndEnsureNotKilled.assert_called_once()
    informer_mock.setFailed.assert_called_once()
    mock_logger.warning.assert_called_once()


def test_qqrunner_update_info_finished_success():
    informer_mock = MagicMock()
    retryer_mock = MagicMock()
    retryer_mock.run.return_value = None

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"
    runner._reloadInfoAndEnsureNotKilled = MagicMock()

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.datetime") as datetime_mock,
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock) as retryer_cls,
    ):
        now = datetime(2024, 1, 1)
        datetime_mock.now.return_value = now

        runner._updateInfoFinished()

    runner._reloadInfoAndEnsureNotKilled.assert_called_once()
    informer_mock.setFinished.assert_called_once_with(now)
    retryer_cls.assert_called_once_with(
        informer_mock.toFile,
        runner._info_file,
        host="random.host.org",
        max_tries=RUNNER_RETRY_TRIES,
        wait_seconds=RUNNER_RETRY_WAIT,
    )
    retryer_mock.run.assert_called_once()
    mock_logger.warning.assert_not_called()


def test_qqrunner_update_info_finished_logs_warning_on_failure():
    informer_mock = MagicMock()
    informer_mock.setFinished.side_effect = Exception("fail")

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"
    runner._reloadInfoAndEnsureNotKilled = MagicMock()

    with patch("qq_lib.run.runner.logger") as mock_logger:
        runner._updateInfoFinished()

    runner._reloadInfoAndEnsureNotKilled.assert_called_once()
    informer_mock.setFinished.assert_called_once()
    mock_logger.warning.assert_called_once()


def test_qqrunner_update_info_running_success():
    informer_mock = MagicMock()
    retryer_mock = MagicMock()
    retryer_mock.run.return_value = None
    nodes = ["node1", "node2"]
    informer_mock.getNodes.return_value = nodes

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"
    runner._work_dir = Path("/workdir")
    runner._reloadInfoAndEnsureNotKilled = MagicMock()

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.datetime") as datetime_mock,
        patch("qq_lib.run.runner.socket.gethostname", return_value="host"),
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock) as retryer_cls,
    ):
        now = datetime(2024, 1, 1)
        datetime_mock.now.return_value = now

        runner._updateInfoRunning()

    runner._reloadInfoAndEnsureNotKilled.assert_called_once()
    informer_mock.setRunning.assert_called_once_with(
        now, "host", nodes, Path("/workdir")
    )
    retryer_cls.assert_called_once_with(
        informer_mock.toFile,
        runner._info_file,
        host="random.host.org",
        max_tries=RUNNER_RETRY_TRIES,
        wait_seconds=RUNNER_RETRY_WAIT,
    )
    retryer_mock.run.assert_called_once()
    mock_logger.debug.assert_called_once()


def test_qqrunner_update_info_running_raises_qqerror_on_failure():
    informer_mock = MagicMock()
    informer_mock.setRunning.side_effect = Exception("fail")

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"
    runner._work_dir = Path("/workdir")
    runner._reloadInfoAndEnsureNotKilled = MagicMock()

    with (
        patch("qq_lib.run.runner.socket.gethostname", return_value="localhost"),
        pytest.raises(QQError, match="Could not update qqinfo file"),
    ):
        runner._updateInfoRunning()


def test_qqrunner_delete_work_dir_invokes_shutil_rmtree_with_retryer():
    runner = QQRunner.__new__(QQRunner)
    runner._work_dir = Path("/scratch/workdir")

    retryer_mock = MagicMock()
    with (
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock) as retryer_cls,
        patch("qq_lib.run.runner.logger") as mock_logger,
    ):
        runner._deleteWorkDir()

    retryer_cls.assert_called_once_with(
        shutil.rmtree,
        Path("/scratch/workdir"),
        max_tries=RUNNER_RETRY_TRIES,
        wait_seconds=RUNNER_RETRY_WAIT,
    )
    retryer_mock.run.assert_called_once()
    mock_logger.debug.assert_called_once_with(
        "Removing working directory '/scratch/workdir'."
    )


def test_qqrunner_set_up_scratch_dir_calls_retryers_with_correct_arguments():
    runner = QQRunner.__new__(QQRunner)
    runner._batch_system = MagicMock()
    runner._informer = MagicMock()
    runner._info_file = Path("job.qqinfo")
    runner._input_dir = Path("/input")
    runner._informer.info.job_id = "123"
    runner._informer.info.excluded_files = ["ignore.txt"]
    runner._informer.info.input_machine = "random.host.org"
    runner._archiver = None

    scratch_dir = Path("/scratch")
    runner._batch_system.getScratchDir.return_value = scratch_dir

    with (
        patch("qq_lib.run.runner.QQRetryer") as retryer_cls,
        patch("qq_lib.run.runner.logger"),
        patch("qq_lib.run.runner.socket.gethostname", return_value="localhost"),
    ):
        runner._setUpScratchDir()

    work_dir = (scratch_dir / SCRATCH_DIR_INNER).resolve()

    # first QQRetryer call: Path.mkdir
    mkdir_call = retryer_cls.call_args_list[0]
    assert mkdir_call.kwargs["max_tries"] == RUNNER_RETRY_TRIES
    assert mkdir_call.kwargs["wait_seconds"] == RUNNER_RETRY_WAIT
    assert mkdir_call.args[0] == Path.mkdir
    assert mkdir_call.args[1] == work_dir

    # second QQRetryer call: os.chdir
    chdir_call = retryer_cls.call_args_list[1]
    assert chdir_call.args[0] == os.chdir
    assert chdir_call.args[1] == work_dir

    # third QQRetryer call: syncWithExclusions
    sync_call = retryer_cls.call_args_list[2]
    expected_excluded = ["ignore.txt", runner._info_file]
    assert sync_call.args[0] == runner._batch_system.syncWithExclusions
    assert sync_call.args[1] == runner._input_dir
    assert sync_call.args[2] == work_dir
    assert sync_call.args[3] == "random.host.org"
    assert sync_call.args[4] == "localhost"
    assert set(sync_call.args[5]) == set(expected_excluded)
    assert sync_call.kwargs["max_tries"] == RUNNER_RETRY_TRIES
    assert sync_call.kwargs["wait_seconds"] == RUNNER_RETRY_WAIT


def test_qqrunner_set_up_scratch_dir_with_archiver_adds_archive_to_excluded():
    runner = QQRunner.__new__(QQRunner)
    runner._batch_system = MagicMock()
    runner._informer = MagicMock()
    runner._info_file = Path("job.qqinfo")
    runner._input_dir = Path("/input")
    runner._informer.info.job_id = "123"
    runner._informer.info.excluded_files = ["ignore.txt"]
    runner._informer.info.input_machine = "random.host.org"

    # set archiver with a dummy _archive attribute
    archiver_mock = MagicMock()
    archiver_mock._archive = Path("storage")
    runner._archiver = archiver_mock

    scratch_dir = Path("/scratch")
    runner._batch_system.getScratchDir.return_value = scratch_dir

    with (
        patch("qq_lib.run.runner.QQRetryer") as retryer_cls,
        patch("qq_lib.run.runner.logger"),
        patch("qq_lib.run.runner.socket.gethostname", return_value="localhost"),
    ):
        runner._setUpScratchDir()

    # ensure QQRetryer was called three times
    assert retryer_cls.call_count == 3

    # verify that the third QQRetryer call (syncWithExclusions) included the archive in excluded
    sync_call_args = retryer_cls.call_args_list[2].args
    excluded_files = sync_call_args[5]
    assert Path("storage") in excluded_files


def test_qqrunner_set_up_shared_dir_calls_chdir_with_input_dir():
    runner = QQRunner.__new__(QQRunner)
    runner._input_dir = Path("/input")

    with patch("qq_lib.run.runner.QQRetryer") as retryer_cls:
        runner._setUpSharedDir()

    call_args = retryer_cls.call_args
    assert call_args.args[0] == os.chdir
    assert call_args.args[1] == runner._input_dir
    assert call_args.kwargs["max_tries"] == RUNNER_RETRY_TRIES
    assert call_args.kwargs["wait_seconds"] == RUNNER_RETRY_WAIT

    assert runner._work_dir == runner._input_dir


def test_qqrunner_log_failure_and_exit_calls_update_and_exits():
    runner = QQRunner.__new__(QQRunner)
    runner._updateInfoFailed = MagicMock()
    exc = RuntimeError("fatal error")
    exc.exit_code = 42  # ty: ignore[unresolved-attribute]

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("sys.exit") as mock_exit,
    ):
        runner.logFailureAndExit(exc)

    runner._updateInfoFailed.assert_called_once_with(42)
    mock_logger.error.assert_called_once_with(exc)
    mock_exit.assert_called_once_with(42)


def test_qqrunner_log_failure_and_exit_calls_fallback_on_exception():
    runner = QQRunner.__new__(QQRunner)
    runner._updateInfoFailed = MagicMock(side_effect=Exception("update failed"))
    exc = RuntimeError("fatal error")
    exc.exit_code = 42  # ty: ignore[unresolved-attribute]

    with patch("qq_lib.run.runner.log_fatal_error_and_exit") as mock_fatal:
        runner.logFailureAndExit(exc)

    runner._updateInfoFailed.assert_called_once_with(42)
    mock_fatal.assert_called_once()


def test_qqrunner_finalize_failure_updates_info_failed():
    runner = QQRunner.__new__(QQRunner)
    runner._process = MagicMock()
    runner._process.returncode = 91
    runner._use_scratch = True
    runner._updateInfoFailed = MagicMock()

    runner.finalize()

    runner._updateInfoFailed.assert_called_once_with(91)


def test_qqrunner_finalize_with_scratch_and_archiver():
    runner = QQRunner.__new__(QQRunner)
    runner._process = MagicMock()
    runner._process.returncode = 0
    runner._archiver = MagicMock()
    runner._use_scratch = True
    runner._work_dir = Path("/work")
    runner._input_dir = Path("/input")
    runner._batch_system = MagicMock()
    runner._informer = MagicMock()
    runner._informer.info.input_machine = "random.host.org"
    runner._informer.info.job_type = QQJobType.STANDARD

    runner._deleteWorkDir = MagicMock()
    runner._updateInfoFinished = MagicMock()

    with (
        patch("qq_lib.run.runner.QQRetryer") as retryer_mock,
        patch("socket.gethostname", return_value="host"),
    ):
        runner.finalize()

    runner._archiver.toArchive.assert_called_once_with(runner._work_dir)
    retryer_mock.assert_called_once()
    runner._deleteWorkDir.assert_called_once()
    runner._updateInfoFinished.assert_called_once()


def test_qqrunner_finalize_with_scratch_and_without_archiver():
    runner = QQRunner.__new__(QQRunner)
    runner._process = MagicMock()
    runner._process.returncode = 0
    runner._archiver = None
    runner._use_scratch = True
    runner._work_dir = Path("/work")
    runner._input_dir = Path("/input")
    runner._batch_system = MagicMock()
    runner._informer = MagicMock()
    runner._informer.info.input_machine = "random.host.org"
    runner._informer.info.job_type = QQJobType.STANDARD

    runner._deleteWorkDir = MagicMock()
    runner._updateInfoFinished = MagicMock()

    with (
        patch("qq_lib.run.runner.QQRetryer") as retryer_mock,
        patch("socket.gethostname", return_value="host"),
    ):
        runner.finalize()

    retryer_mock.assert_called_once()
    runner._deleteWorkDir.assert_called_once()
    runner._updateInfoFinished.assert_called_once()


def test_qqrunner_finalize_without_scratch_and_with_archiver():
    runner = QQRunner.__new__(QQRunner)
    runner._process = MagicMock()
    runner._process.returncode = 0
    runner._archiver = MagicMock()
    runner._use_scratch = False
    runner._work_dir = Path("/work")
    runner._input_dir = Path("/input")
    runner._batch_system = MagicMock()
    runner._informer = MagicMock()
    runner._informer.info.input_machine = "random.host.org"
    runner._informer.info.job_type = QQJobType.STANDARD

    runner._deleteWorkDir = MagicMock()
    runner._updateInfoFinished = MagicMock()

    runner.finalize()

    runner._archiver.toArchive.assert_called_once_with(runner._work_dir)
    runner._deleteWorkDir.assert_not_called()
    runner._updateInfoFinished.assert_called_once()


def test_qqrunner_finalize_without_scratch_and_without_archiver():
    runner = QQRunner.__new__(QQRunner)
    runner._process = MagicMock()
    runner._process.returncode = 0
    runner._archiver = None
    runner._use_scratch = False
    runner._work_dir = Path("/work")
    runner._input_dir = Path("/input")
    runner._batch_system = MagicMock()
    runner._informer = MagicMock()
    runner._informer.info.input_machine = "random.host.org"
    runner._informer.info.job_type = QQJobType.STANDARD

    runner._deleteWorkDir = MagicMock()
    runner._updateInfoFinished = MagicMock()

    runner.finalize()

    runner._deleteWorkDir.assert_not_called()
    runner._updateInfoFinished.assert_called_once()


def test_qqrunner_finalize_with_scratch_archiver_and_resubmit():
    runner = QQRunner.__new__(QQRunner)
    runner._process = MagicMock()
    runner._process.returncode = 0
    runner._archiver = MagicMock()
    runner._use_scratch = True
    runner._work_dir = Path("/work")
    runner._input_dir = Path("/input")
    runner._batch_system = MagicMock()
    runner._informer = MagicMock()
    runner._informer.info.input_machine = "random.host.org"
    runner._informer.info.job_type = QQJobType.LOOP

    runner._deleteWorkDir = MagicMock()
    runner._updateInfoFinished = MagicMock()
    runner._resubmit = MagicMock()

    with (
        patch("qq_lib.run.runner.QQRetryer") as retryer_mock,
        patch("socket.gethostname", return_value="host"),
    ):
        runner.finalize()

    runner._archiver.toArchive.assert_called_once_with(runner._work_dir)
    retryer_mock.assert_called_once()
    runner._deleteWorkDir.assert_called_once()
    runner._resubmit.assert_called_once()


def test_qqrunner_execute_updates_info_and_runs_script(tmp_path):
    script_file = tmp_path / "script.sh"
    script_file.write_text("#!/bin/bash\necho Hello\n")

    stdout_file = tmp_path / "stdout.log"
    stderr_file = tmp_path / "stderr.log"

    runner = QQRunner.__new__(QQRunner)
    runner._updateInfoRunning = MagicMock()
    runner._informer = MagicMock()
    runner._informer.info.script_name = str(script_file)
    runner._informer.info.stdout_file = stdout_file
    runner._informer.info.stderr_file = stderr_file
    runner._process = None

    mock_popen = MagicMock()
    mock_popen.returncode = 0

    with (
        patch(
            "qq_lib.run.runner.subprocess.Popen", return_value=mock_popen
        ) as popen_mock,
        patch("pathlib.Path.open", create=True) as open_mock,
    ):
        # mock context manager for stdout/stderr files
        mock_file = MagicMock()
        open_mock.return_value.__enter__.return_value = mock_file

        retcode = runner.execute()

    runner._updateInfoRunning.assert_called_once()
    popen_mock.assert_called_once_with(
        ["bash"],
        stdin=subprocess.PIPE,
        stdout=mock_file,
        stderr=mock_file,
        text=True,
    )
    assert runner._process == mock_popen
    assert retcode == 0


def test_qqrunner_prepare_with_scratch_and_archiver():
    runner = QQRunner.__new__(QQRunner)
    runner._use_scratch = True
    runner._archiver = MagicMock()
    runner._setUpScratchDir = MagicMock()
    runner._setUpSharedDir = MagicMock()
    runner._informer = MagicMock()
    runner._informer.info.loop_info.current = 2
    runner._work_dir = "/tmp/work"

    runner.prepare()

    runner._setUpScratchDir.assert_called_once()
    runner._setUpSharedDir.assert_not_called()
    runner._archiver.fromArchive.assert_called_once_with("/tmp/work", 2)


def test_qqrunner_prepare_with_scratch_and_without_archiver():
    runner = QQRunner.__new__(QQRunner)
    runner._use_scratch = True
    runner._archiver = None
    runner._setUpScratchDir = MagicMock()
    runner._setUpSharedDir = MagicMock()

    runner.prepare()

    runner._setUpScratchDir.assert_called_once()
    runner._setUpSharedDir.assert_not_called()


def test_qqrunner_prepare_without_scratch_and_with_archiver():
    runner = QQRunner.__new__(QQRunner)
    runner._use_scratch = False
    runner._archiver = MagicMock()
    runner._setUpScratchDir = MagicMock()
    runner._setUpSharedDir = MagicMock()
    runner._informer = MagicMock()
    runner._informer.info.loop_info.current = 5
    runner._work_dir = "/tmp/work_shared"

    runner.prepare()

    runner._setUpSharedDir.assert_called_once()
    runner._setUpScratchDir.assert_not_called()
    runner._archiver.fromArchive.assert_called_once_with("/tmp/work_shared", 5)


def test_qqrunner_prepare_without_scratch_and_without_archiver():
    runner = QQRunner.__new__(QQRunner)
    runner._use_scratch = False
    runner._archiver = None
    runner._setUpScratchDir = MagicMock()
    runner._setUpSharedDir = MagicMock()

    runner.prepare()

    runner._setUpSharedDir.assert_called_once()
    runner._setUpScratchDir.assert_not_called()


def test_log_fatal_error_and_exit_known_exception():
    exc = QQRunFatalError("fatal")

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        pytest.raises(SystemExit) as e,
    ):
        log_fatal_error_and_exit(exc)

    mock_logger.error.assert_any_call("Fatal qq run error: fatal")
    mock_logger.error.assert_any_call(
        "Failure state was NOT logged into the job info file."
    )
    assert e.value.code == QQRunFatalError.exit_code


def test_log_fatal_error_and_exit_unknown_exception():
    exc = RuntimeError("unknown")

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        pytest.raises(SystemExit) as e,
    ):
        log_fatal_error_and_exit(exc)

    mock_logger.error.assert_any_call("Fatal qq run error: unknown")
    mock_logger.error.assert_any_call(
        "Failure state was NOT logged into the job info file."
    )
    mock_logger.critical.assert_called_once_with(exc, exc_info=True, stack_info=True)
    assert e.value.code == UNEXPECTED_EXCEPTION_EXIT_CODE
