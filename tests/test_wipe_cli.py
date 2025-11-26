# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.wipe.cli import _wipe_work_dir, wipe


@patch("qq_lib.wipe.cli.logger.info")
@patch("qq_lib.wipe.cli.Wiper.fromInformer")
def test_wipe_work_dir_success_with_force(mock_wiper_from_informer, mock_logger_info):
    mock_wiper = MagicMock()
    mock_wiper.wipe.return_value = "job123"
    mock_wiper_from_informer.return_value = mock_wiper

    informer = MagicMock()
    _wipe_work_dir(informer, force=True, yes=False)

    mock_wiper.ensureSuitable.assert_not_called()
    mock_wiper.wipe.assert_called_once()
    mock_logger_info.assert_called_with(
        "Deleted the working directory of the job 'job123'."
    )


@patch("qq_lib.wipe.cli.logger.info")
@patch("qq_lib.wipe.cli.Wiper.fromInformer")
@patch("qq_lib.wipe.cli.yes_or_no_prompt", return_value=True)
def test_wipe_work_dir_success_with_prompt(
    mock_prompt, mock_wiper_from_informer, mock_logger_info
):
    mock_wiper = MagicMock()
    mock_wiper.wipe.return_value = "jobXYZ"
    mock_wiper_from_informer.return_value = mock_wiper

    informer = MagicMock()
    _wipe_work_dir(informer, force=False, yes=False)

    mock_wiper.ensureSuitable.assert_called_once()
    mock_wiper.wipe.assert_called_once()
    mock_prompt.assert_called_once()
    mock_logger_info.assert_called_with(
        "Deleted the working directory of the job 'jobXYZ'."
    )


@patch("qq_lib.wipe.cli.logger.info")
@patch("qq_lib.wipe.cli.Wiper.fromInformer")
@patch("qq_lib.wipe.cli.yes_or_no_prompt", return_value=False)
def test_wipe_work_dir_aborts_on_negative_prompt(
    mock_prompt, mock_wiper_from_informer, mock_logger_info
):
    mock_wiper = MagicMock()
    mock_wiper_from_informer.return_value = mock_wiper

    informer = MagicMock()
    _wipe_work_dir(informer, force=False, yes=False)

    mock_wiper.ensureSuitable.assert_called_once()
    mock_wiper.wipe.assert_not_called()
    mock_prompt.assert_called_once()
    mock_logger_info.assert_called_with("Operation aborted.")


@patch("qq_lib.wipe.cli.Wiper.fromInformer")
def test_wipe_work_dir_raises_not_suitable_error(mock_wiper_from_informer):
    mock_wiper = MagicMock()
    mock_wiper.ensureSuitable.side_effect = QQNotSuitableError("Unsuitable job")
    mock_wiper_from_informer.return_value = mock_wiper

    informer = MagicMock()
    with pytest.raises(QQNotSuitableError, match="Unsuitable job"):
        _wipe_work_dir(informer, force=False, yes=True)


@patch("qq_lib.wipe.cli.Wiper.fromInformer")
def test_wipe_work_dir_raises_general_error(mock_wiper_from_informer):
    mock_wiper = MagicMock()
    mock_wiper.wipe.side_effect = QQError("Cannot delete working directory")
    mock_wiper_from_informer.return_value = mock_wiper

    informer = MagicMock()
    with pytest.raises(QQError, match="Cannot delete working directory"):
        _wipe_work_dir(informer, force=True, yes=True)


def test_wipe_invokes_repeater_and_exits_success(tmp_path):
    dummy_file = tmp_path / "info.qq"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    informer_mock = MagicMock()

    with (
        patch("qq_lib.wipe.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.wipe.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.wipe.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.wipe.cli.logger"),
    ):
        result = runner.invoke(wipe, [])

    assert result.exit_code == 0
    calls = [c[0][0] for c in repeater_mock.onException.call_args_list]
    assert QQNotSuitableError in calls
    assert QQError in calls
    repeater_mock.run.assert_called_once()


def test_wipe_invokes_repeater_with_job_id_and_exits_success():
    runner = CliRunner()
    repeater_mock = MagicMock()
    informer_mock = MagicMock()

    with (
        patch("qq_lib.wipe.cli.Informer.fromJobId", return_value=informer_mock),
        patch("qq_lib.wipe.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.wipe.cli.logger"),
    ):
        result = runner.invoke(wipe, ["123"])

    assert result.exit_code == 0
    calls = [c[0][0] for c in repeater_mock.onException.call_args_list]
    assert QQNotSuitableError in calls
    assert QQError in calls
    repeater_mock.run.assert_called_once()


def test_wipe_catches_qqerror_and_exits_91(tmp_path):
    dummy_file = tmp_path / "info.qq"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    informer_mock = MagicMock()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = QQError("wipe failure")

    with (
        patch("qq_lib.wipe.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.wipe.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.wipe.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.wipe.cli.logger") as mock_logger,
    ):
        result = runner.invoke(wipe, [])

    assert result.exit_code == CFG.exit_codes.default
    mock_logger.error.assert_called_once_with(repeater_mock.run.side_effect)


def test_wipe_catches_generic_exception_and_exits_99(tmp_path):
    dummy_file = tmp_path / "info.qq"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    informer_mock = MagicMock()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = Exception("unexpected wipe crash")

    with (
        patch("qq_lib.wipe.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.wipe.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.wipe.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.wipe.cli.logger") as mock_logger,
    ):
        result = runner.invoke(wipe, [])

    assert result.exit_code == CFG.exit_codes.unexpected_error
    mock_logger.critical.assert_called_once()
