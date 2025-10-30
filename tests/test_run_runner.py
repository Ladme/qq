# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import shutil
import signal
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.error import (
    QQError,
    QQJobMismatchError,
    QQRunCommunicationError,
    QQRunFatalError,
)
from qq_lib.properties.job_type import QQJobType
from qq_lib.properties.states import NaiveState
from qq_lib.run.runner import CFG, QQRunner, log_fatal_error_and_exit


def test_qq_runner_init_success():
    with (
        patch("qq_lib.run.runner.signal.signal") as mock_signal,
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch(
            "qq_lib.run.runner.socket.gethostname", return_value="mockhost"
        ) as mock_socket,
        patch("qq_lib.run.runner.qq_lib.__version__", "1.0.0"),
        patch("qq_lib.run.runner.QQBatchMeta.fromEnvVarOrGuess") as mock_batchmeta,
        patch("qq_lib.run.runner.QQRetryer") as mock_retryer,
    ):
        batch = MagicMock()
        batch.getJobId.return_value = "12345"
        mock_batchmeta.return_value = batch

        informer = MagicMock()
        informer.matchesJob.return_value = True
        informer.batch_system = batch
        informer.info.job_id = "12345"
        informer.info.input_dir = "/tmp/input"
        informer.info.input_machine = "input_host"
        informer.info.loop_info = None
        informer.usesScratch.return_value = False

        retryer = MagicMock()
        retryer.run.return_value = informer
        mock_retryer.return_value = retryer

        runner = QQRunner(Path("job.qqinfo"), "input_host")

        mock_signal.assert_called_once()
        mock_batchmeta.assert_called_once()
        mock_retryer.assert_called_once()
        retryer.run.assert_called_once()
        mock_logger.info.assert_called_once()
        mock_socket.assert_called_once()

        assert runner._batch_system == batch
        assert runner._informer == informer
        assert runner._info_file == Path("job.qqinfo")
        assert runner._input_machine == "input_host"
        assert str(runner._input_dir) == "/tmp/input"
        assert runner._use_scratch is False
        assert runner._archiver is None
        assert runner._process is None


def test_qqrunner_init_raises_when_get_job_id_missing():
    with (
        patch("qq_lib.run.runner.signal.signal"),
        patch("qq_lib.run.runner.QQBatchMeta.fromEnvVarOrGuess") as mock_meta,
    ):
        batch = MagicMock()
        batch.getJobId.return_value = None
        mock_meta.return_value = batch

        with pytest.raises(QQRunFatalError, match="Job has no associated job id"):
            QQRunner(Path("job.qqinfo"), "host")

        mock_meta.assert_called_once()
        batch.getJobId.assert_called_once()


def test_qq_runner_init_raises_on_batchmeta_failure():
    with (
        patch("qq_lib.run.runner.signal.signal"),
        patch(
            "qq_lib.run.runner.QQBatchMeta.fromEnvVarOrGuess",
            side_effect=Exception("boom"),
        ),
        pytest.raises(QQRunFatalError, match="Unable to load valid qq info file"),
    ):
        QQRunner(Path("job.qqinfo"), "host")


def test_qq_runner_init_raises_on_job_mismatch():
    with (
        patch("qq_lib.run.runner.signal.signal"),
        patch("qq_lib.run.runner.QQBatchMeta.fromEnvVarOrGuess") as mock_batchmeta,
        patch("qq_lib.run.runner.QQRetryer") as mock_retryer,
    ):
        batch = MagicMock()
        batch.getJobId.return_value = "12345"
        mock_batchmeta.return_value = batch

        informer = MagicMock()
        informer.matchesJob.return_value = False
        informer.batch_system = batch
        informer.info.job_id = "99999"

        retryer = MagicMock()
        retryer.run.return_value = informer
        mock_retryer.return_value = retryer

        with pytest.raises(QQRunFatalError, match="Info file does not correspond"):
            QQRunner(Path("job.qqinfo"), "host")

        mock_batchmeta.assert_called_once()
        mock_retryer.assert_called_once()
        retryer.run.assert_called_once()


def test_qq_runner_init_raises_on_batch_system_mismatch():
    with (
        patch("qq_lib.run.runner.signal.signal"),
        patch("qq_lib.run.runner.QQBatchMeta.fromEnvVarOrGuess") as mock_batchmeta,
        patch("qq_lib.run.runner.QQRetryer") as mock_retryer,
    ):
        batch = MagicMock()
        batch.getJobId.return_value = "12345"
        mock_batchmeta.return_value = batch

        informer = MagicMock()
        informer.matchesJob.return_value = True
        informer.batch_system = MagicMock()
        informer.info.job_id = "12345"

        retryer = MagicMock()
        retryer.run.return_value = informer
        mock_retryer.return_value = retryer

        with pytest.raises(QQRunFatalError, match="Batch system mismatch"):
            QQRunner(Path("job.qqinfo"), "host")

        mock_batchmeta.assert_called_once()
        mock_retryer.assert_called_once()
        retryer.run.assert_called_once()


def test_qq_runner_init_creates_archiver_when_loop_info_present():
    with (
        patch("qq_lib.run.runner.signal.signal"),
        patch("qq_lib.run.runner.QQBatchMeta.fromEnvVarOrGuess") as mock_batchmeta,
        patch("qq_lib.run.runner.QQRetryer") as mock_retryer,
        patch("qq_lib.run.runner.QQArchiver") as mock_archiver,
    ):
        batch = MagicMock()
        batch.getJobId.return_value = "12345"
        mock_batchmeta.return_value = batch

        loop_info = MagicMock()
        informer = MagicMock()
        informer.matchesJob.return_value = True
        informer.batch_system = batch
        informer.info.job_id = "12345"
        informer.info.input_dir = "/tmp/input"
        informer.info.input_machine = "input_host"
        informer.info.loop_info = loop_info
        informer.usesScratch.return_value = True

        retryer = MagicMock()
        retryer.run.return_value = informer
        mock_retryer.return_value = retryer

        runner = QQRunner(Path("job.qqinfo"), "host")

        mock_archiver.assert_called_once_with(
            loop_info.archive,
            loop_info.archive_format,
            informer.info.input_machine,
            informer.info.input_dir,
            batch,
        )
        mock_batchmeta.assert_called_once()
        mock_retryer.assert_called_once()
        retryer.run.assert_called_once()

        assert runner._archiver is not None
        assert runner._use_scratch is True
        assert str(runner._input_dir) == "/tmp/input"
        assert runner._batch_system == batch
        assert runner._informer == informer


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

    def terminate_and_stop():
        process_mock.poll.return_value = 0

    process_mock.terminate.side_effect = terminate_and_stop
    runner._process = process_mock

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.sleep") as mock_sleep,
        patch("qq_lib.run.runner.CFG") as cfg_mock,
    ):
        cfg_mock.runner.sigterm_to_sigkill = 3
        runner._cleanup()

    runner._updateInfoKilled.assert_called_once()
    mock_logger.info.assert_called_once_with("Cleaning up: terminating subprocess.")
    process_mock.terminate.assert_called_once()
    mock_sleep.assert_called_once_with(3)
    process_mock.kill.assert_not_called()


def test_qqrunner_cleanup_with_timeout():
    runner = QQRunner.__new__(QQRunner)
    runner._updateInfoKilled = MagicMock()
    process_mock = MagicMock()
    process_mock.poll.return_value = None
    runner._process = process_mock

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.sleep") as mock_sleep,
    ):
        runner._cleanup()

    runner._updateInfoKilled.assert_called_once()
    mock_logger.info.assert_any_call("Cleaning up: terminating subprocess.")
    process_mock.terminate.assert_called_once()
    mock_sleep.assert_called_once_with(CFG.runner.sigterm_to_sigkill)
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
    runner._should_resubmit = True

    with patch("qq_lib.run.runner.logger") as mock_logger:
        runner._resubmit()

    mock_logger.info.assert_called_once_with(
        "This was the final cycle of the loop job. Not resubmitting."
    )


def test_qqrunner_resubmit_should_resubmit_is_false():
    informer_mock = MagicMock()
    informer_mock.info.loop_info.current = 5
    informer_mock.info.loop_info.end = 9999

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._should_resubmit = False

    with patch("qq_lib.run.runner.logger") as mock_logger:
        runner._resubmit()

    mock_logger.info.assert_called_once_with(
        f"The script finished with an exit code of '{CFG.exit_codes.qq_run_no_resubmit}' indicating that the next cycle of the job should not be submitted. Not resubmitting."
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
    runner._should_resubmit = True

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
        max_tries=CFG.runner.retry_tries,
        wait_seconds=CFG.runner.retry_wait,
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
    runner._should_resubmit = True

    with (
        patch("qq_lib.run.runner.QQRetryer", side_effect=QQError("resubmit failed")),
        patch("qq_lib.run.runner.logger"),
        pytest.raises(QQError, match="resubmit failed"),
    ):
        runner._resubmit()


def test_qqrunner_update_info_killed_success():
    informer_mock = MagicMock()
    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"
    runner._reloadInfoAndEnsureValid = MagicMock()

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.datetime") as datetime_mock,
    ):
        now = datetime(2024, 1, 1)
        datetime_mock.now.return_value = now

        runner._updateInfoKilled()

    runner._reloadInfoAndEnsureValid.assert_called_with(retry=False)
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
    runner._reloadInfoAndEnsureValid = MagicMock()

    with patch("qq_lib.run.runner.logger") as mock_logger:
        runner._updateInfoKilled()

    runner._reloadInfoAndEnsureValid.assert_called_with(retry=False)
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
    runner._reloadInfoAndEnsureValid = MagicMock()

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.datetime") as datetime_mock,
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock) as retryer_cls,
    ):
        now = datetime(2024, 1, 1)
        datetime_mock.now.return_value = now

        runner._updateInfoFailed(42)

    runner._reloadInfoAndEnsureValid.assert_called_once()
    informer_mock.setFailed.assert_called_once_with(now, 42)
    retryer_cls.assert_called_once_with(
        informer_mock.toFile,
        runner._info_file,
        host="random.host.org",
        max_tries=CFG.runner.retry_tries,
        wait_seconds=CFG.runner.retry_wait,
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
    runner._reloadInfoAndEnsureValid = MagicMock()

    with patch("qq_lib.run.runner.logger") as mock_logger:
        runner._updateInfoFailed(99)

    runner._reloadInfoAndEnsureValid.assert_called_once()
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
    runner._reloadInfoAndEnsureValid = MagicMock()

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.datetime") as datetime_mock,
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock) as retryer_cls,
    ):
        now = datetime(2024, 1, 1)
        datetime_mock.now.return_value = now

        runner._updateInfoFinished()

    runner._reloadInfoAndEnsureValid.assert_called_once()
    informer_mock.setFinished.assert_called_once_with(now)
    retryer_cls.assert_called_once_with(
        informer_mock.toFile,
        runner._info_file,
        host="random.host.org",
        max_tries=CFG.runner.retry_tries,
        wait_seconds=CFG.runner.retry_wait,
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
    runner._reloadInfoAndEnsureValid = MagicMock()

    with patch("qq_lib.run.runner.logger") as mock_logger:
        runner._updateInfoFinished()

    runner._reloadInfoAndEnsureValid.assert_called_once()
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
    runner._reloadInfoAndEnsureValid = MagicMock()

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.datetime") as datetime_mock,
        patch("qq_lib.run.runner.socket.gethostname", return_value="host"),
        patch("qq_lib.run.runner.QQRetryer", return_value=retryer_mock) as retryer_cls,
    ):
        now = datetime(2024, 1, 1)
        datetime_mock.now.return_value = now

        runner._updateInfoRunning()

    runner._reloadInfoAndEnsureValid.assert_called_once()
    informer_mock.setRunning.assert_called_once_with(
        now, "host", nodes, Path("/workdir")
    )
    retryer_cls.assert_called_once_with(
        informer_mock.toFile,
        runner._info_file,
        host="random.host.org",
        max_tries=CFG.runner.retry_tries,
        wait_seconds=CFG.runner.retry_wait,
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
    runner._reloadInfoAndEnsureValid = MagicMock()

    with (
        patch("qq_lib.run.runner.socket.gethostname", return_value="localhost"),
        pytest.raises(QQError, match="Could not update qqinfo file"),
    ):
        runner._updateInfoRunning()


def test_qqrunner_update_info_running_raises_on_empty_node_list():
    informer_mock = MagicMock()
    informer_mock.getNodes.return_value = []

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer_mock
    runner._info_file = Path("job.qqinfo")
    runner._input_machine = "random.host.org"
    runner._work_dir = Path("/workdir")
    runner._reloadInfoAndEnsureValid = MagicMock()

    with (
        patch("qq_lib.run.runner.socket.gethostname", return_value="localhost"),
        pytest.raises(
            QQError, match="Could not get the list of used nodes from the batch server"
        ),
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
        max_tries=CFG.runner.retry_tries,
        wait_seconds=CFG.runner.retry_wait,
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

    work_dir = (scratch_dir / CFG.runner.scratch_dir_inner).resolve()

    # first QQRetryer call: Path.mkdir
    mkdir_call = retryer_cls.call_args_list[0]
    assert mkdir_call.kwargs["max_tries"] == CFG.runner.retry_tries
    assert mkdir_call.kwargs["wait_seconds"] == CFG.runner.retry_wait
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
    assert sync_call.kwargs["max_tries"] == CFG.runner.retry_tries
    assert sync_call.kwargs["wait_seconds"] == CFG.runner.retry_wait


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
    assert call_args.kwargs["max_tries"] == CFG.runner.retry_tries
    assert call_args.kwargs["wait_seconds"] == CFG.runner.retry_wait

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

    mock_process = MagicMock()
    # poll() returns None twice, then 0 (finished)
    mock_process.poll.side_effect = [None, None, 0]
    mock_process.returncode = 0

    with (
        patch(
            "qq_lib.run.runner.subprocess.Popen", return_value=mock_process
        ) as popen_mock,
        patch("qq_lib.run.runner.Path.open", create=True) as open_mock,
        patch("qq_lib.run.runner.sleep") as sleep_mock,
        patch("qq_lib.run.runner.logger"),
        patch("qq_lib.run.runner.CFG") as cfg_mock,
    ):
        cfg_mock.runner.subprocess_checks_wait_time = 0.1
        mock_file = MagicMock()
        open_mock.return_value.__enter__.return_value = mock_file

        retcode = runner.execute()

    runner._updateInfoRunning.assert_called_once()
    popen_mock.assert_called_once_with(
        ["bash", str(script_file.resolve())],
        stdout=mock_file,
        stderr=mock_file,
        text=True,
    )
    sleep_mock.assert_called()
    assert retcode == 0


def test_qqrunner_execute_handles_no_resubmit_exit_code(tmp_path):
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
    runner._informer.info.loop_info = MagicMock()
    runner._should_resubmit = True

    mock_process = MagicMock()
    mock_process.poll.side_effect = [None, None, 95]
    mock_process.returncode = 95

    with (
        patch(
            "qq_lib.run.runner.subprocess.Popen", return_value=mock_process
        ) as popen_mock,
        patch("qq_lib.run.runner.Path.open", create=True) as open_mock,
        patch("qq_lib.run.runner.sleep") as sleep_mock,
        patch("qq_lib.run.runner.logger"),
        patch("qq_lib.run.runner.CFG") as cfg_mock,
    ):
        cfg_mock.runner.subprocess_checks_wait_time = 0.1
        cfg_mock.exit_codes.qq_run_no_resubmit = 95
        mock_file = MagicMock()
        open_mock.return_value.__enter__.return_value = mock_file

        retcode = runner.execute()

    runner._updateInfoRunning.assert_called_once()
    popen_mock.assert_called_once_with(
        ["bash", str(script_file.resolve())],
        stdout=mock_file,
        stderr=mock_file,
        text=True,
    )
    sleep_mock.assert_called()
    assert not runner._should_resubmit
    assert retcode == 0


def test_qqrunner_prepare_with_scratch_and_archiver():
    runner = QQRunner.__new__(QQRunner)
    runner._use_scratch = True
    runner._archiver = MagicMock()
    runner._setUpScratchDir = MagicMock()
    runner._setUpSharedDir = MagicMock()
    runner._informer = MagicMock()
    runner._informer.info.loop_info.current = 2
    runner._informer.info.script_name = "run_job"
    runner._work_dir = "/tmp/work"

    with (
        patch("qq_lib.run.runner.logger") as mock_logger,
        patch("qq_lib.run.runner.CFG") as mock_cfg,
    ):
        mock_cfg.loop_jobs.pattern = "_loop_%d+"
        runner.prepare()

    runner._archiver.makeArchiveDir.assert_called_once()
    runner._archiver.archiveRunTimeFiles.assert_called_once_with("run_job_loop_1\\+", 1)
    runner._setUpScratchDir.assert_called_once()
    runner._setUpSharedDir.assert_not_called()
    runner._archiver.fromArchive.assert_called_once_with("/tmp/work", 2)
    mock_logger.debug.assert_any_call("Archiving run time files from cycle 1.")


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
    runner._informer.info.script_name = "task"
    runner._work_dir = "/tmp/work_shared"

    with (
        patch("qq_lib.run.runner.logger"),
        patch("qq_lib.run.runner.CFG") as mock_cfg,
    ):
        mock_cfg.loop_jobs.pattern = "_loop_%d+"
        runner.prepare()

    runner._archiver.makeArchiveDir.assert_called_once()
    runner._archiver.archiveRunTimeFiles.assert_called_once_with("task_loop_4\\+", 4)
    runner._setUpSharedDir.assert_called_once()
    runner._setUpScratchDir.assert_not_called()


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
    assert e.value.code == CFG.exit_codes.unexpected_error


@patch("qq_lib.run.runner.QQRetryer")
@patch("qq_lib.run.runner.QQInformer")
def test_qq_runner_reload_info_with_retry(mock_informer_cls, mock_retryer_cls):
    mock_retryer = MagicMock()
    mock_informer = MagicMock()
    mock_retryer.run.return_value = mock_informer
    mock_retryer_cls.return_value = mock_retryer

    runner = QQRunner.__new__(QQRunner)
    runner._info_file = "job.qqinfo"
    runner._input_machine = "host"

    runner._reloadInfo(retry=True)

    mock_retryer_cls.assert_called_once_with(
        mock_informer_cls.fromFile,
        "job.qqinfo",
        host="host",
        max_tries=CFG.runner.retry_tries,
        wait_seconds=CFG.runner.retry_wait,
    )
    mock_retryer.run.assert_called_once()
    assert runner._informer == mock_informer


@patch("qq_lib.run.runner.QQRetryer")
@patch("qq_lib.run.runner.QQInformer")
def test_qq_runner_reload_info_without_retry(mock_informer_cls, mock_retryer_cls):
    mock_informer = MagicMock()
    mock_informer_cls.fromFile.return_value = mock_informer

    runner = QQRunner.__new__(QQRunner)
    runner._info_file = "job.qqinfo"
    runner._input_machine = "host"

    runner._reloadInfo(retry=False)

    mock_informer_cls.fromFile.assert_called_once_with("job.qqinfo", "host")
    mock_retryer_cls.assert_not_called()
    assert runner._informer == mock_informer


def test_qq_runner_ensure_matches_job_with_matching_numeric_id():
    informer = MagicMock()
    informer.info.job_id = "12345.cluster.domain"
    informer.matchesJob = (
        lambda job_id: informer.info.job_id.split(".", 1)[0] == job_id.split(".", 1)[0]
    )

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer
    runner._info_file = "job.qqinfo"

    runner._ensureMatchesJob("12345")


def test_qq_runner_ensure_matches_job_with_different_numeric_id_raises():
    informer = MagicMock()
    informer.info.job_id = "99999.cluster.domain"
    informer.matchesJob = (
        lambda job_id: informer.info.job_id.split(".", 1)[0] == job_id.split(".", 1)[0]
    )

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer
    runner._info_file = "job.qqinfo"

    with pytest.raises(QQJobMismatchError, match="job.qqinfo"):
        runner._ensureMatchesJob("12345")


def test_qq_runner_ensure_matches_job_with_partial_suffix_matching():
    informer = MagicMock()
    informer.info.job_id = "5678.random.server.org"
    informer.matchesJob = (
        lambda job_id: informer.info.job_id.split(".", 1)[0] == job_id.split(".", 1)[0]
    )

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer
    runner._info_file = "job.qqinfo"

    runner._ensureMatchesJob("5678")


def test_qq_runner_ensure_not_killed_passes_when_not_killed():
    informer = MagicMock()
    informer.info.job_state = NaiveState.RUNNING

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer

    runner._ensureNotKilled()

    assert informer.info.job_state == NaiveState.RUNNING


def test_qq_runner_ensure_not_killed_raises_when_killed():
    informer = MagicMock()
    informer.info.job_state = NaiveState.KILLED

    runner = QQRunner.__new__(QQRunner)
    runner._informer = informer

    with pytest.raises(QQRunCommunicationError, match="Job has been killed"):
        runner._ensureNotKilled()


def test_qq_runner_reload_info_and_ensure_valid_calls_all_methods():
    runner = QQRunner.__new__(QQRunner)
    runner._informer = MagicMock()
    runner._informer.info.job_id = "12345"

    runner._reloadInfo = MagicMock()
    runner._ensureMatchesJob = MagicMock()
    runner._ensureNotKilled = MagicMock()

    runner._reloadInfoAndEnsureValid(retry=True)

    runner._reloadInfo.assert_called_once_with(True)
    runner._ensureMatchesJob.assert_called_once_with("12345")
    runner._ensureNotKilled.assert_called_once()


def test_qq_runner_reload_info_and_ensure_valid_raises_on_job_mismatch():
    runner = QQRunner.__new__(QQRunner)
    runner._informer = MagicMock()
    runner._informer.info.job_id = "12345"

    runner._reloadInfo = MagicMock()
    runner._ensureMatchesJob = MagicMock(side_effect=QQJobMismatchError("Mismatch"))
    runner._ensureNotKilled = MagicMock()

    with pytest.raises(QQJobMismatchError, match="Mismatch"):
        runner._reloadInfoAndEnsureValid(retry=False)

    runner._reloadInfo.assert_called_once_with(False)
    runner._ensureNotKilled.assert_not_called()


def test_qq_runner_reload_info_and_ensure_valid_raises_on_killed_state():
    runner = QQRunner.__new__(QQRunner)
    runner._informer = MagicMock()
    runner._informer.info.job_id = "12345"

    runner._reloadInfo = MagicMock()
    runner._ensureMatchesJob = MagicMock()
    runner._ensureNotKilled = MagicMock(side_effect=QQRunCommunicationError("Killed"))

    with pytest.raises(QQRunCommunicationError, match="Killed"):
        runner._reloadInfoAndEnsureValid()

    runner._reloadInfo.assert_called_once_with(False)
    runner._ensureMatchesJob.assert_called_once_with("12345")
