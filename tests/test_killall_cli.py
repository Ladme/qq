# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import getpass
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from qq_lib.batch.interface.job import BatchJobInterface
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.kill.cli import kill_job
from qq_lib.killall.cli import (
    _informers_from_jobs,
    _log_error_and_continue,
    killall,
)


def test_informers_from_jobs():
    job_good = MagicMock()
    job_bad = MagicMock()
    job_good2 = MagicMock()

    informer1 = MagicMock()
    informer2 = MagicMock()

    with patch(
        "qq_lib.killall.cli.Informer.fromBatchJob",
        side_effect=[informer1, QQError(), informer2],
    ):
        result = _informers_from_jobs([job_good, job_bad, job_good2])

    assert result == [informer1, informer2]


def test_informers_from_jobs_no_jobs():
    result = _informers_from_jobs([])
    assert result == []


def test_killall_no_jobs_exits_zero():
    runner = CliRunner()
    with (
        patch("qq_lib.killall.cli.BatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli.logger") as logger_mock,
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedBatchJobs.return_value = []

        result = runner.invoke(killall)

        batch_system.getUnfinishedBatchJobs.assert_called_once_with(getpass.getuser())
        logger_mock.info.assert_called_once_with(
            "You have no active jobs. Nothing to kill."
        )
        assert result.exit_code == 0


def test_killall_jobs_but_no_info_files_exits_zero():
    runner = CliRunner()
    job_mock = MagicMock(spec=BatchJobInterface)

    with (
        patch("qq_lib.killall.cli.BatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._informers_from_jobs", return_value=[]),
        patch("qq_lib.killall.cli.logger") as logger_mock,
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedBatchJobs.return_value = [job_mock]

        result = runner.invoke(killall)

        logger_mock.info.assert_called_once_with(
            "You have no active qq jobs (and 1 other jobs). Nothing to kill."
        )
        assert result.exit_code == 0


def test_killall_yes_flag_invokes_repeater():
    informer_mock = MagicMock()
    runner = CliRunner()
    repeater_mock = MagicMock()

    job_mock = MagicMock(spec=BatchJobInterface)

    with (
        patch("qq_lib.killall.cli.BatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._informers_from_jobs", return_value=[informer_mock]),
        patch(
            "qq_lib.killall.cli.Repeater", return_value=repeater_mock
        ) as repeater_cls,
        patch("qq_lib.killall.cli.logger"),
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedBatchJobs.return_value = [job_mock]

        result = runner.invoke(killall, ["--yes"])

        repeater_mock.onException.assert_any_call(
            QQNotSuitableError, _log_error_and_continue
        )
        repeater_mock.onException.assert_any_call(QQError, _log_error_and_continue)
        repeater_cls.assert_called_once_with(
            [informer_mock],
            kill_job,
            force=False,
            yes=True,
        )
        repeater_mock.run.assert_called_once()
        assert result.exit_code == 0


def test_killall_force_flag_invokes_repeater():
    informer_mock = MagicMock()
    runner = CliRunner()
    repeater_mock = MagicMock()

    job_mock = MagicMock(spec=BatchJobInterface)

    with (
        patch("qq_lib.killall.cli.BatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._informers_from_jobs", return_value=[informer_mock]),
        patch(
            "qq_lib.killall.cli.Repeater", return_value=repeater_mock
        ) as repeater_cls,
        patch("qq_lib.killall.cli.logger"),
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedBatchJobs.return_value = [job_mock]

        result = runner.invoke(killall, ["--force"])

        repeater_cls.assert_called_once_with(
            [informer_mock],
            kill_job,
            force=True,
            yes=True,
        )
        repeater_mock.run.assert_called_once()
        assert result.exit_code == 0


def test_killall_user_prompt_yes(monkeypatch):
    informer_mock = MagicMock()
    runner = CliRunner()
    repeater_mock = MagicMock()

    monkeypatch.setattr("qq_lib.killall.cli.yes_or_no_prompt", lambda _msg: True)

    job_mock = MagicMock(spec=BatchJobInterface)

    with (
        patch("qq_lib.killall.cli.BatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._informers_from_jobs", return_value=[informer_mock]),
        patch(
            "qq_lib.killall.cli.Repeater", return_value=repeater_mock
        ) as repeater_cls,
        patch("qq_lib.killall.cli.logger"),
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedBatchJobs.return_value = [job_mock]

        result = runner.invoke(killall)

        repeater_cls.assert_called_once_with(
            [informer_mock],
            kill_job,
            force=False,
            yes=True,
        )
        repeater_mock.run.assert_called_once()
        assert result.exit_code == 0


def test_killall_user_prompt_no(monkeypatch):
    informer_mock = MagicMock()
    runner = CliRunner()

    monkeypatch.setattr("qq_lib.killall.cli.yes_or_no_prompt", lambda _msg: False)

    job_mock = MagicMock(spec=BatchJobInterface)

    with (
        patch("qq_lib.killall.cli.BatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._informers_from_jobs", return_value=[informer_mock]),
        patch("qq_lib.killall.cli.logger") as logger_mock,
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedBatchJobs.return_value = [job_mock]

        result = runner.invoke(killall)

        logger_mock.info.assert_called_with("Operation aborted.")
        assert result.exit_code == 0


def test_killall_qqerror_in_main_loop_exits_91():
    informer_mock = MagicMock()
    runner = CliRunner()

    with (
        patch("qq_lib.killall.cli.BatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._informers_from_jobs", return_value=[informer_mock]),
        patch("qq_lib.killall.cli.Repeater", side_effect=QQError("fail")),
        patch("qq_lib.killall.cli.logger"),
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedBatchJobs.return_value = [MagicMock()]

        result = runner.invoke(killall, ["--yes"])

        assert result.exit_code == CFG.exit_codes.default


def test_killall_generic_exception_exits_99():
    informer_mock = MagicMock()
    runner = CliRunner()

    def raise_exception(*_args, **_kwargs):
        raise RuntimeError("unexpected")

    with (
        patch("qq_lib.killall.cli.BatchMeta.fromEnvVarOrGuess") as batch_meta_mock,
        patch("qq_lib.killall.cli._informers_from_jobs", return_value=[informer_mock]),
        patch("qq_lib.killall.cli.Repeater", side_effect=raise_exception),
        patch("qq_lib.killall.cli.logger"),
    ):
        batch_system = batch_meta_mock.return_value
        batch_system.getUnfinishedBatchJobs.return_value = [MagicMock()]

        result = runner.invoke(killall, ["--yes"])

        assert result.exit_code == CFG.exit_codes.unexpected_error
