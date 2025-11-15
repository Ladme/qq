# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.kill.cli import kill, kill_job


def test_kill_job_force_skips_suitability_and_logs_killed():
    with (
        patch("qq_lib.kill.cli.Killer.fromInformer") as mock_killer_ctor,
        patch("qq_lib.kill.cli.logger.info") as mock_logger,
        patch("qq_lib.kill.cli.yes_or_no_prompt") as mock_prompt,
        patch("qq_lib.kill.cli.console"),
    ):
        mock_killer = MagicMock()
        mock_killer.terminate.return_value = "1234"
        mock_killer_ctor.return_value = mock_killer

        kill_job(MagicMock(), force=True, yes=False)

        mock_killer.ensureSuitable.assert_not_called()
        mock_killer.terminate.assert_called_once_with(True)
        mock_prompt.assert_not_called()
        mock_logger.assert_called_once_with("Killed the job '1234'.")


def test_kill_job_prompts_yes_and_kills():
    with (
        patch("qq_lib.kill.cli.Killer.fromInformer") as mock_killer_ctor,
        patch("qq_lib.kill.cli.logger.info") as mock_logger,
        patch("qq_lib.kill.cli.console"),
        patch("qq_lib.kill.cli.yes_or_no_prompt", return_value=True),
    ):
        mock_killer = MagicMock()
        mock_killer.terminate.return_value = "5678"
        mock_killer_ctor.return_value = mock_killer

        kill_job(MagicMock(), force=False, yes=False)

        mock_killer.ensureSuitable.assert_called_once()
        mock_killer.terminate.assert_called_once_with(False)
        mock_logger.assert_called_once_with("Killed the job '5678'.")


def test_kill_job_prompts_no_and_aborts():
    with (
        patch("qq_lib.kill.cli.Killer.fromInformer") as mock_killer_ctor,
        patch("qq_lib.kill.cli.logger.info") as mock_logger,
        patch("qq_lib.kill.cli.console"),
        patch("qq_lib.kill.cli.yes_or_no_prompt", return_value=False),
    ):
        mock_killer = MagicMock()
        mock_killer_ctor.return_value = mock_killer

        kill_job(MagicMock(), force=False, yes=False)

        mock_killer.terminate.assert_not_called()
        mock_logger.assert_called_once_with("Operation aborted.")


def test_kill_invokes_repeater_and_exits_success(tmp_path):
    dummy_file = tmp_path / "info.qq"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    informer_mock = MagicMock()

    with (
        patch("qq_lib.kill.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.kill.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.kill.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.kill.cli.logger"),
    ):
        result = runner.invoke(kill, [])

    assert result.exit_code == 0

    calls = [c[0][0] for c in repeater_mock.onException.call_args_list]
    assert QQNotSuitableError in calls
    assert QQError in calls

    repeater_mock.run.assert_called_once()


def test_kill_catches_qqerror_and_exits_91(tmp_path):
    dummy_file = tmp_path / "info.qq"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    informer_mock = MagicMock()
    repeater_mock.run.side_effect = QQError("error occurred")

    with (
        patch("qq_lib.kill.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.kill.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.kill.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.kill.cli.logger") as mock_logger,
    ):
        result = runner.invoke(kill, [])

    assert result.exit_code == CFG.exit_codes.default
    mock_logger.error.assert_called_once_with(repeater_mock.run.side_effect)


def test_kill_catches_generic_exception_and_exits_99(tmp_path):
    dummy_file = tmp_path / "info.qq"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    informer_mock = MagicMock()
    repeater_mock.run.side_effect = Exception("critical error")

    with (
        patch("qq_lib.kill.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.kill.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.kill.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.kill.cli.logger") as mock_logger,
    ):
        result = runner.invoke(kill, [])

    assert result.exit_code == CFG.exit_codes.unexpected_error
    mock_logger.critical.assert_called_once()


def test_kill_with_job_id_invokes_repeater():
    runner = CliRunner()
    repeater_mock = MagicMock()
    informer_mock = MagicMock()

    with (
        patch("qq_lib.kill.cli.Informer.fromJobId", return_value=informer_mock),
        patch("qq_lib.kill.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.kill.cli.logger"),
    ):
        result = runner.invoke(kill, ["12345"])

    assert result.exit_code == 0
    repeater_mock.run.assert_called_once()
