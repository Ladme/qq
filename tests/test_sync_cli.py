# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.sync.cli import _split_files, _sync_job, sync


def test_sync_job_calls_sync():
    informer = MagicMock()
    syncer_mock = MagicMock()
    with (
        patch("qq_lib.sync.cli.Syncer.fromInformer", return_value=syncer_mock),
        patch("qq_lib.sync.cli.console"),
    ):
        _sync_job(informer, ["a.txt", "b.txt"])

    syncer_mock.printInfo.assert_called_once()
    syncer_mock.ensureSuitable.assert_called_once()
    syncer_mock.sync.assert_called_once_with(["a.txt", "b.txt"])


def test_sync_job_calls_sync_without_files():
    informer = MagicMock()
    syncer_mock = MagicMock()
    with (
        patch("qq_lib.sync.cli.Syncer.fromInformer", return_value=syncer_mock),
        patch("qq_lib.sync.cli.console"),
    ):
        _sync_job(informer, None)

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
    informer_mock = MagicMock()

    with (
        patch("qq_lib.sync.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.sync.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.sync.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.sync.cli.logger"),
    ):
        result = runner.invoke(sync, [])

    assert result.exit_code == 0
    calls = [c[0][0] for c in repeater_mock.onException.call_args_list]
    assert QQNotSuitableError in calls
    assert QQError in calls
    repeater_mock.run.assert_called_once()


def test_sync_invokes_repeater_with_job_id_and_exits_success():
    runner = CliRunner()
    repeater_mock = MagicMock()
    informer_mock = MagicMock()

    with (
        patch("qq_lib.sync.cli.Informer.fromJobId", return_value=informer_mock),
        patch("qq_lib.sync.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.sync.cli.logger"),
    ):
        result = runner.invoke(sync, ["123"])

    assert result.exit_code == 0
    calls = [c[0][0] for c in repeater_mock.onException.call_args_list]
    assert QQNotSuitableError in calls
    assert QQError in calls
    repeater_mock.run.assert_called_once()


def test_sync_catches_qqerror_and_exits_91(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    informer_mock = MagicMock()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = QQError("error occurred")

    with (
        patch("qq_lib.sync.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.sync.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.sync.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.sync.cli.logger") as mock_logger,
    ):
        result = runner.invoke(sync, [])

    assert result.exit_code == CFG.exit_codes.default
    mock_logger.error.assert_called_once_with(repeater_mock.run.side_effect)


def test_sync_catches_generic_exception_and_exits_99(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    informer_mock = MagicMock()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = Exception("fatal error")

    with (
        patch("qq_lib.sync.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.sync.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.sync.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.sync.cli.logger") as mock_logger,
    ):
        result = runner.invoke(sync, [])

    assert result.exit_code == CFG.exit_codes.unexpected_error
    mock_logger.critical.assert_called_once()
