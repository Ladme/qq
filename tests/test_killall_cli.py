# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import getpass
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from qq_lib.batch.interface.job import BatchJobInfoInterface
from qq_lib.core.error import QQError, QQJobMismatchError, QQNotSuitableError
from qq_lib.kill.cli import kill_job
from qq_lib.killall.cli import _jobs_to_paths, _log_error_and_continue, killall


def test_jobs_to_paths():
    mock_job_with_file = MagicMock()
    mock_job_with_file.getInfoFile.return_value = Path("/some/path1.qqinfo")

    mock_job_without_file = MagicMock()
    mock_job_without_file.getInfoFile.return_value = None

    mock_job_with_file2 = MagicMock()
    mock_job_with_file2.getInfoFile.return_value = Path("/some/path2.qqinfo")

    jobs = [mock_job_with_file, mock_job_without_file, mock_job_with_file2]

    result = _jobs_to_paths(jobs)

    assert result == [Path("/some/path1.qqinfo"), Path("/some/path2.qqinfo")]

    for job in jobs:
        job.getInfoFile.assert_called_once()


def test_jobs_to_paths_no_paths():
    result = _jobs_to_paths([])

    assert result == []


def test_killall_no_jobs_exits_zero():
    runner = CliRunner()
    with (
        patch("qq_lib.killall.cli.QQBatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli.logger") as logger_mock,
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedJobsInfo.return_value = []

        result = runner.invoke(killall)

        batch_system.getUnfinishedJobsInfo.assert_called_once_with(getpass.getuser())
        logger_mock.info.assert_called_once_with(
            "You have no active jobs. Nothing to kill."
        )
        assert result.exit_code == 0


def test_killall_jobs_but_no_info_files_exits_zero():
    runner = CliRunner()
    job_mock = MagicMock(spec=BatchJobInfoInterface)
    job_mock.getInfoFile.return_value = None

    with (
        patch("qq_lib.killall.cli.QQBatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._jobs_to_paths", return_value=[]),
        patch("qq_lib.killall.cli.logger") as logger_mock,
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedJobsInfo.return_value = [job_mock]

        result = runner.invoke(killall)

        logger_mock.info.assert_called_once_with(
            "You have no active qq jobs (and 1 other jobs). Nothing to kill."
        )
        assert result.exit_code == 0


def test_killall_yes_flag_invokes_repeater(tmp_path):
    job_file = tmp_path / "job1.qq"
    job_file.write_text("dummy content")
    runner = CliRunner()
    repeater_mock = MagicMock()

    job_mock = MagicMock(spec=BatchJobInfoInterface)
    job_mock.getInfoFile.return_value = job_file

    with (
        patch("qq_lib.killall.cli.QQBatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._jobs_to_paths", return_value=[job_file]),
        patch(
            "qq_lib.killall.cli.QQRepeater", return_value=repeater_mock
        ) as repeater_cls,
        patch("qq_lib.killall.cli.logger"),
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedJobsInfo.return_value = [job_mock]

        result = runner.invoke(killall, ["--yes"])

        repeater_mock.onException.assert_any_call(
            QQJobMismatchError, _log_error_and_continue
        )
        repeater_mock.onException.assert_any_call(
            QQNotSuitableError, _log_error_and_continue
        )
        repeater_mock.onException.assert_any_call(QQError, _log_error_and_continue)
        repeater_cls.assert_called_once_with(
            [job_file],
            kill_job,
            force=False,
            yes=True,
            job=None,
        )
        repeater_mock.run.assert_called_once()
        assert result.exit_code == 0


def test_killall_force_flag_invokes_repeater(tmp_path):
    job_file = tmp_path / "job2.qq"
    job_file.write_text("dummy")
    runner = CliRunner()
    repeater_mock = MagicMock()

    job_mock = MagicMock(spec=BatchJobInfoInterface)
    job_mock.getInfoFile.return_value = job_file

    with (
        patch("qq_lib.killall.cli.QQBatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._jobs_to_paths", return_value=[job_file]),
        patch(
            "qq_lib.killall.cli.QQRepeater", return_value=repeater_mock
        ) as repeater_cls,
        patch("qq_lib.killall.cli.logger"),
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedJobsInfo.return_value = [job_mock]

        result = runner.invoke(killall, ["--force"])

        repeater_cls.assert_called_once_with(
            [job_file],
            kill_job,
            force=True,
            yes=True,
            job=None,
        )
        repeater_mock.run.assert_called_once()
        assert result.exit_code == 0


def test_killall_user_prompt_yes(monkeypatch, tmp_path):
    job_file = tmp_path / "job3.qq"
    job_file.write_text("dummy")
    runner = CliRunner()
    repeater_mock = MagicMock()

    monkeypatch.setattr("qq_lib.killall.cli.yes_or_no_prompt", lambda _msg: True)

    job_mock = MagicMock(spec=BatchJobInfoInterface)
    job_mock.getInfoFile.return_value = job_file

    with (
        patch("qq_lib.killall.cli.QQBatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._jobs_to_paths", return_value=[job_file]),
        patch(
            "qq_lib.killall.cli.QQRepeater", return_value=repeater_mock
        ) as repeater_cls,
        patch("qq_lib.killall.cli.logger"),
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedJobsInfo.return_value = [job_mock]

        result = runner.invoke(killall)

        repeater_cls.assert_called_once_with(
            [job_file],
            kill_job,
            force=False,
            yes=True,
            job=None,
        )
        repeater_mock.run.assert_called_once()
        assert result.exit_code == 0


def test_killall_user_prompt_no(monkeypatch, tmp_path):
    job_file = tmp_path / "job4.qq"
    job_file.write_text("dummy")
    runner = CliRunner()

    monkeypatch.setattr("qq_lib.killall.cli.yes_or_no_prompt", lambda _msg: False)

    job_mock = MagicMock(spec=BatchJobInfoInterface)
    job_mock.getInfoFile.return_value = job_file

    with (
        patch("qq_lib.killall.cli.QQBatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._jobs_to_paths", return_value=[job_file]),
        patch("qq_lib.killall.cli.logger") as logger_mock,
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedJobsInfo.return_value = [job_mock]

        result = runner.invoke(killall)

        logger_mock.info.assert_called_with("Operation aborted.")
        assert result.exit_code == 0


def test_killall_qqerror_in_main_loop_exits_91(tmp_path):
    runner = CliRunner()
    job_file = tmp_path / "job5.qq"
    job_file.write_text("dummy content")

    job_mock = MagicMock(spec=BatchJobInfoInterface)
    job_mock.getInfoFile.return_value = job_file

    with (
        patch("qq_lib.killall.cli.QQBatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._jobs_to_paths", return_value=[job_file]),
        patch("qq_lib.killall.cli.QQRepeater", side_effect=QQError("fail")),
        patch("qq_lib.killall.cli.logger"),
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedJobsInfo.return_value = [job_mock]

        result = runner.invoke(killall, ["--yes"])

        assert result.exit_code == 91


def test_killall_generic_exception_exits_99(tmp_path):
    runner = CliRunner()
    job_file = tmp_path / "job6.qq"
    job_file.write_text("dummy content")

    job_mock = MagicMock(spec=BatchJobInfoInterface)
    job_mock.getInfoFile.return_value = job_file

    def raise_exception(*_args, **_kwargs):
        raise RuntimeError("unexpected")

    with (
        patch("qq_lib.killall.cli.QQBatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._jobs_to_paths", return_value=[job_file]),
        patch("qq_lib.killall.cli.QQRepeater", side_effect=raise_exception),
        patch("qq_lib.killall.cli.logger"),
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedJobsInfo.return_value = [job_mock]

        result = runner.invoke(killall, ["--yes"])

        assert result.exit_code == 99
