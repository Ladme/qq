# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.core.error import QQError, QQJobMismatchError, QQNotSuitableError
from qq_lib.sync.cli import _split_files, _sync_job, sync


def test_sync_job(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy content")

    syncer_mock = MagicMock()
    syncer_mock.matchesJob.return_value = True

    with patch("qq_lib.sync.cli.QQSyncer", return_value=syncer_mock):
        _sync_job(dummy_file, job="12345", files=["a.txt", "b.txt"])

    syncer_mock.matchesJob.assert_called_once_with("12345")
    syncer_mock.printInfo.assert_called_once()
    syncer_mock.ensureSuitable.assert_called_once()
    syncer_mock.sync.assert_called_once_with(["a.txt", "b.txt"])


def test_sync_job_raises_job_mismatch(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy content")

    syncer_mock = MagicMock()
    syncer_mock.matchesJob.return_value = False

    with (
        patch("qq_lib.sync.cli.QQSyncer", return_value=syncer_mock),
        pytest.raises(
            QQJobMismatchError, match="Info file for job '12345' does not exist."
        ),
    ):
        _sync_job(dummy_file, job="12345", files=None)


def test_sync_job_calls_sync_without_files(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy content")

    syncer_mock = MagicMock()
    syncer_mock.matchesJob.return_value = True

    with patch("qq_lib.sync.cli.QQSyncer", return_value=syncer_mock):
        _sync_job(dummy_file, job=None, files=None)

    syncer_mock.sync.assert_called_once_with(None)


def test_split_files_returns_none_when_input_none():
    assert _split_files(None) is None


def test_split_files_returns_none_when_input_empty_string():
    assert _split_files("") is None


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("a.txt", ["a.txt"]),
        ("a.txt b.txt", ["a.txt", "b.txt"]),
        ("a.txt,b.txt", ["a.txt", "b.txt"]),
        ("a.txt:b.txt", ["a.txt", "b.txt"]),
        ("a.txt , b.txt:c.txt  d.txt", ["a.txt", "b.txt", "c.txt", "d.txt"]),
    ],
)
def test_split_files_splits_correctly(input_str, expected):
    assert _split_files(input_str) == expected


def test_sync_invokes_repeater_and_exits_success(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()

    with (
        patch(
            "qq_lib.sync.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
        patch("qq_lib.sync.cli.QQRepeater", return_value=repeater_mock),
        patch("qq_lib.sync.cli.logger"),
    ):
        result = runner.invoke(sync, [])

        assert result.exit_code == 0
        calls = [call[0][0] for call in repeater_mock.onException.call_args_list]
        assert QQJobMismatchError in calls
        assert QQNotSuitableError in calls
        assert QQError in calls
        repeater_mock.run.assert_called_once()


def test_sync_catches_qqerror_and_exits_91(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = QQError("error occurred")

    with (
        patch(
            "qq_lib.sync.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
        patch("qq_lib.sync.cli.QQRepeater", return_value=repeater_mock),
        patch("qq_lib.sync.cli.logger") as mock_logger,
    ):
        result = runner.invoke(sync, [])

        assert result.exit_code == 91
        mock_logger.error.assert_called_once_with(repeater_mock.run.side_effect)


def test_sync_catches_generic_exception_and_exits_99(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = Exception("fatal error")

    with (
        patch(
            "qq_lib.sync.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
        patch("qq_lib.sync.cli.QQRepeater", return_value=repeater_mock),
        patch("qq_lib.sync.cli.logger") as mock_logger,
    ):
        result = runner.invoke(sync, [])

        assert result.exit_code == 99
        mock_logger.critical.assert_called_once()
