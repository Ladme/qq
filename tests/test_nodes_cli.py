# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.nodes.cli import nodes


def test_nodes_command_prints_available_nodes():
    runner = CliRunner()
    mock_node = MagicMock()
    mock_node.isAvailableToUser.return_value = True

    with (
        patch("qq_lib.nodes.cli.BatchMeta.fromEnvVarOrGuess") as mock_meta,
        patch("qq_lib.nodes.cli.NodesPresenter") as mock_presenter_cls,
        patch("qq_lib.nodes.cli.Console"),
        patch("qq_lib.nodes.cli.getpass.getuser", return_value="user"),
    ):
        mock_batch = MagicMock()
        mock_batch.getNodes.return_value = [mock_node]
        mock_meta.return_value = mock_batch

        mock_presenter = MagicMock()
        mock_presenter_cls.return_value = mock_presenter

        result = runner.invoke(nodes, [])

    assert result.exit_code == 0
    mock_meta.assert_called_once()
    mock_batch.getNodes.assert_called_once()
    mock_presenter_cls.assert_called_once_with([mock_node], "user", False)
    mock_presenter.createNodesInfoPanel.assert_called_once()


def test_nodes_command_prints_all_nodes_with_flag():
    runner = CliRunner()
    mock_node = MagicMock()

    with (
        patch("qq_lib.nodes.cli.BatchMeta.fromEnvVarOrGuess") as mock_meta,
        patch("qq_lib.nodes.cli.NodesPresenter") as mock_presenter_cls,
        patch("qq_lib.nodes.cli.Console"),
        patch("qq_lib.nodes.cli.getpass.getuser", return_value="testuser"),
    ):
        mock_batch = MagicMock()
        mock_batch.getNodes.return_value = [mock_node]
        mock_meta.return_value = mock_batch

        mock_presenter = MagicMock()
        mock_presenter_cls.return_value = mock_presenter

        result = runner.invoke(nodes, ["--all"])

    assert result.exit_code == 0
    mock_meta.assert_called_once()
    mock_batch.getNodes.assert_called_once()
    mock_presenter_cls.assert_called_once_with([mock_node], "testuser", True)
    mock_presenter.createNodesInfoPanel.assert_called_once()


def test_nodes_command_outputs_yaml_when_flag_set():
    runner = CliRunner()
    mock_node = MagicMock()
    mock_node.isAvailableToUser.return_value = True

    with (
        patch("qq_lib.nodes.cli.BatchMeta.fromEnvVarOrGuess") as mock_meta,
        patch("qq_lib.nodes.cli.NodesPresenter") as mock_presenter_cls,
        patch("qq_lib.nodes.cli.getpass.getuser", return_value="testuser"),
    ):
        mock_batch = MagicMock()
        mock_batch.getNodes.return_value = [mock_node]
        mock_meta.return_value = mock_batch

        mock_presenter = MagicMock()
        mock_presenter_cls.return_value = mock_presenter

        result = runner.invoke(nodes, ["--yaml"])

    assert result.exit_code == 0
    mock_meta.assert_called_once()
    mock_batch.getNodes.assert_called_once()
    mock_presenter_cls.assert_called_once_with([mock_node], "testuser", False)
    mock_presenter.dumpYaml.assert_called_once()


def test_nodes_command_handles_qqerror_and_exits_91():
    runner = CliRunner()

    with (
        patch(
            "qq_lib.nodes.cli.BatchMeta.fromEnvVarOrGuess",
            side_effect=QQError("error"),
        ),
        patch("qq_lib.nodes.cli.logger") as mock_logger,
    ):
        result = runner.invoke(nodes, [])

    assert result.exit_code == CFG.exit_codes.default
    mock_logger.error.assert_called_once()


def test_nodes_command_handles_unexpected_exception_and_exits_99():
    runner = CliRunner()

    with (
        patch(
            "qq_lib.nodes.cli.BatchMeta.fromEnvVarOrGuess",
            side_effect=RuntimeError("fatal"),
        ),
        patch("qq_lib.nodes.cli.logger") as mock_logger,
    ):
        result = runner.invoke(nodes, [])

    assert result.exit_code == CFG.exit_codes.unexpected_error
    mock_logger.critical.assert_called_once()
