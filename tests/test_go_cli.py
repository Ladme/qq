# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.go.cli import _go_to_job, go


def test_go_to_job_calls_printinfo_ensure_suitable_and_go():
    informer = MagicMock()
    goer_mock = MagicMock()

    with (
        patch("qq_lib.go.cli.Goer.fromInformer", return_value=goer_mock),
        patch("qq_lib.go.cli.console", new=MagicMock()),
    ):
        _go_to_job(informer)

    goer_mock.printInfo.assert_called_once()
    goer_mock.ensureSuitable.assert_called_once()
    goer_mock.go.assert_called_once()


def test_go_invokes_repeater_and_exits_success(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    informer_mock = MagicMock()

    with (
        patch("qq_lib.go.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.go.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.go.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.go.cli.logger"),
    ):
        result = runner.invoke(go, [])

    assert result.exit_code == 0
    calls = [call_args[0][0] for call_args in repeater_mock.onException.call_args_list]
    assert QQNotSuitableError in calls
    assert QQError in calls
    repeater_mock.run.assert_called_once()


def test_go_invokes_repeater_and_exits_success_with_job_id():
    runner = CliRunner()
    repeater_mock = MagicMock()
    informer_mock = MagicMock()

    with (
        patch("qq_lib.go.cli.Informer.fromJobId", return_value=informer_mock),
        patch("qq_lib.go.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.go.cli.logger"),
    ):
        result = runner.invoke(go, ["12345"])

    assert result.exit_code == 0
    calls = [call_args[0][0] for call_args in repeater_mock.onException.call_args_list]
    assert QQNotSuitableError in calls
    assert QQError in calls
    repeater_mock.run.assert_called_once()


def test_go_catches_qqerror_and_exits_91(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = QQError("error occurred")
    informer_mock = MagicMock()

    with (
        patch("qq_lib.go.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.go.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.go.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.go.cli.logger") as mock_logger,
    ):
        result = runner.invoke(go, [])

    assert result.exit_code == CFG.exit_codes.default
    mock_logger.error.assert_called_once_with(repeater_mock.run.side_effect)


def test_go_catches_generic_exception_and_exits_99(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = Exception("fatal error")
    informer_mock = MagicMock()

    with (
        patch("qq_lib.go.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.go.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.go.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.go.cli.logger") as mock_logger,
    ):
        result = runner.invoke(go, [])

    assert result.exit_code == CFG.exit_codes.unexpected_error
    mock_logger.critical.assert_called_once()
