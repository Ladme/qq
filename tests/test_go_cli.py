# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQJobMismatchError, QQNotSuitableError
from qq_lib.go.cli import _go_to_job, go


def test_go_to_job_matches_job_raises_mismatch_error():
    info_file = Path("/fake/job.qqinfo")
    goer_mock = MagicMock()
    goer_mock.matchesJob.return_value = False

    with (
        patch("qq_lib.go.cli.QQGoer", return_value=goer_mock),
        pytest.raises(
            QQJobMismatchError,
            match="Info file for job '12345' does not exist or is not reachable.",
        ),
    ):
        _go_to_job(info_file, job="12345")


def test_go_to_job_calls_printinfo_ensure_suitable_and_go():
    info_file = Path("/fake/job.qqinfo")
    goer_mock = MagicMock()
    goer_mock.matchesJob.return_value = True

    with (
        patch("qq_lib.go.cli.QQGoer", return_value=goer_mock),
        patch("qq_lib.go.cli.console", new=MagicMock()),
    ):
        _go_to_job(info_file, job=None)

    goer_mock.printInfo.assert_called_once()
    goer_mock.ensureSuitable.assert_called_once()
    goer_mock.go.assert_called_once()


def test_go_invokes_repeater_and_exits_success(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()

    with (
        patch(
            "qq_lib.go.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
        patch("qq_lib.go.cli.QQRepeater", return_value=repeater_mock),
        patch("qq_lib.go.cli.logger"),
    ):
        result = runner.invoke(go, [])

        assert result.exit_code == 0
        calls = [call[0][0] for call in repeater_mock.onException.call_args_list]
        assert QQJobMismatchError in calls
        assert QQNotSuitableError in calls
        assert QQError in calls
        repeater_mock.run.assert_called_once()


def test_go_catches_qqerror_and_exits_91(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = QQError("error occurred")

    with (
        patch(
            "qq_lib.go.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
        patch("qq_lib.go.cli.QQRepeater", return_value=repeater_mock),
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

    with (
        patch(
            "qq_lib.go.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
        patch("qq_lib.go.cli.QQRepeater", return_value=repeater_mock),
        patch("qq_lib.go.cli.logger") as mock_logger,
    ):
        result = runner.invoke(go, [])

        assert result.exit_code == CFG.exit_codes.unexpected_error
        mock_logger.critical.assert_called_once()
