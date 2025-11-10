# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.queues import queues


def test_queues_command_prints_available_queues():
    runner = CliRunner()
    mock_queue = MagicMock()
    mock_queue.isAvailableToUser.return_value = True

    with (
        patch("qq_lib.queues.cli.BatchMeta.fromEnvVarOrGuess") as mock_meta,
        patch("qq_lib.queues.cli.QueuesPresenter") as mock_presenter_cls,
        patch("qq_lib.queues.cli.Console"),
        patch("qq_lib.queues.cli.getpass.getuser", return_value="user"),
    ):
        mock_batch = MagicMock()
        mock_batch.getQueues.return_value = [mock_queue]
        mock_meta.return_value = mock_batch

        mock_presenter = MagicMock()
        mock_presenter_cls.return_value = mock_presenter

        result = runner.invoke(queues, [])

    assert result.exit_code == 0
    mock_meta.assert_called_once()
    mock_batch.getQueues.assert_called_once()
    mock_presenter_cls.assert_called_once_with([mock_queue], "user", False)
    mock_presenter.createQueuesInfoPanel.assert_called_once()


def test_queues_command_prints_all_queues_with_flag():
    runner = CliRunner()
    mock_queue = MagicMock()

    with (
        patch("qq_lib.queues.cli.BatchMeta.fromEnvVarOrGuess") as mock_meta,
        patch("qq_lib.queues.cli.QueuesPresenter") as mock_presenter_cls,
        patch("qq_lib.queues.cli.Console"),
        patch("qq_lib.queues.cli.getpass.getuser", return_value="testuser"),
    ):
        mock_batch = MagicMock()
        mock_batch.getQueues.return_value = [mock_queue]
        mock_meta.return_value = mock_batch

        mock_presenter = MagicMock()
        mock_presenter_cls.return_value = mock_presenter

        result = runner.invoke(queues, ["--all"])

    assert result.exit_code == 0
    mock_presenter_cls.assert_called_once_with([mock_queue], "testuser", True)
    mock_presenter.createQueuesInfoPanel.assert_called_once()


def test_queues_command_outputs_yaml_when_flag_set():
    runner = CliRunner()
    mock_queue = MagicMock()
    mock_queue.isAvailableToUser.return_value = True

    with (
        patch("qq_lib.queues.cli.BatchMeta.fromEnvVarOrGuess") as mock_meta,
        patch("qq_lib.queues.cli.QueuesPresenter") as mock_presenter_cls,
        patch("qq_lib.queues.cli.getpass.getuser", return_value="testuser"),
    ):
        mock_batch = MagicMock()
        mock_batch.getQueues.return_value = [mock_queue]
        mock_meta.return_value = mock_batch

        mock_presenter = MagicMock()
        mock_presenter_cls.return_value = mock_presenter

        result = runner.invoke(queues, ["--yaml"])

    assert result.exit_code == 0
    mock_presenter.dumpYaml.assert_called_once()


def test_queues_command_handles_qqerror_and_exits_91():
    runner = CliRunner()

    with (
        patch(
            "qq_lib.queues.cli.BatchMeta.fromEnvVarOrGuess",
            side_effect=QQError("error"),
        ),
        patch("qq_lib.queues.cli.logger") as mock_logger,
    ):
        result = runner.invoke(queues, [])

    assert result.exit_code == CFG.exit_codes.default
    mock_logger.error.assert_called_once()


def test_queues_command_handles_unexpected_exception_and_exits_99():
    runner = CliRunner()

    with (
        patch(
            "qq_lib.queues.cli.BatchMeta.fromEnvVarOrGuess",
            side_effect=RuntimeError("fatal"),
        ),
        patch("qq_lib.queues.cli.logger") as mock_logger,
    ):
        result = runner.invoke(queues, [])

    assert result.exit_code == CFG.exit_codes.unexpected_error
    mock_logger.critical.assert_called_once()
