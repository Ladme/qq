# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQJobMismatchError, QQNotSuitableError
from qq_lib.wipe.cli import wipe, wipe_work_dir


@patch("qq_lib.wipe.cli.logger.info")
@patch("qq_lib.wipe.cli.Wiper")
def test_wipe_work_dir_success_with_force(mock_wiper_cls, mock_logger_info):
    mock_wiper = MagicMock()
    mock_wiper.matchesJob.return_value = True
    mock_wiper.delete.return_value = "job123"
    mock_wiper_cls.return_value = mock_wiper

    wipe_work_dir(Path("job.qqinfo"), force=True, yes=False, job=None)

    mock_wiper.ensureSuitable.assert_not_called()
    mock_wiper.delete.assert_called_once()
    mock_logger_info.assert_called_with(
        "Deleted the working directory of the job 'job123'."
    )


@patch("qq_lib.wipe.cli.logger.info")
@patch("qq_lib.wipe.cli.Wiper")
@patch("qq_lib.wipe.cli.yes_or_no_prompt", return_value=True)
def test_wipe_work_dir_success_with_prompt(
    mock_prompt, mock_wiper_cls, mock_logger_info
):
    mock_wiper = MagicMock()
    mock_wiper.matchesJob.return_value = True
    mock_wiper.delete.return_value = "jobXYZ"
    mock_wiper_cls.return_value = mock_wiper

    wipe_work_dir(Path("job.qqinfo"), force=False, yes=False, job=None)

    mock_wiper.ensureSuitable.assert_called_once()
    mock_wiper.delete.assert_called_once()
    mock_prompt.assert_called_once()
    mock_logger_info.assert_called_with(
        "Deleted the working directory of the job 'jobXYZ'."
    )


@patch("qq_lib.wipe.cli.logger.info")
@patch("qq_lib.wipe.cli.Wiper")
@patch("qq_lib.wipe.cli.yes_or_no_prompt", return_value=False)
def test_wipe_work_dir_aborts_on_negative_prompt(
    mock_prompt, mock_wiper_cls, mock_logger_info
):
    mock_wiper = MagicMock()
    mock_wiper.matchesJob.return_value = True
    mock_wiper_cls.return_value = mock_wiper

    wipe_work_dir(Path("job.qqinfo"), force=False, yes=False, job=None)

    mock_wiper.ensureSuitable.assert_called_once()
    mock_wiper.delete.assert_not_called()
    mock_prompt.assert_called_once()
    mock_logger_info.assert_called_with("Operation aborted.")


@patch("qq_lib.wipe.cli.Wiper")
def test_wipe_work_dir_raises_job_mismatch(mock_wiper_cls):
    mock_wiper = MagicMock()
    mock_wiper.matchesJob.return_value = False
    mock_wiper_cls.return_value = mock_wiper

    with pytest.raises(
        QQJobMismatchError, match="Info file for job 'jobABC' does not exist."
    ):
        wipe_work_dir(Path("job.qqinfo"), force=False, yes=False, job="jobABC")


@patch("qq_lib.wipe.cli.Wiper")
def test_wipe_work_dir_raises_not_suitable_error(mock_wiper_cls):
    mock_wiper = MagicMock()
    mock_wiper.matchesJob.return_value = True
    mock_wiper.ensureSuitable.side_effect = QQNotSuitableError("Unsuitable job")
    mock_wiper_cls.return_value = mock_wiper

    with pytest.raises(QQNotSuitableError, match="Unsuitable job"):
        wipe_work_dir(Path("job.qqinfo"), force=False, yes=True, job=None)


@patch("qq_lib.wipe.cli.Wiper")
def test_wipe_work_dir_raises_general_error(mock_wiper_cls):
    mock_wiper = MagicMock()
    mock_wiper.matchesJob.return_value = True
    mock_wiper.delete.side_effect = QQError("Cannot delete working directory")
    mock_wiper_cls.return_value = mock_wiper

    with pytest.raises(QQError, match="Cannot delete working directory"):
        wipe_work_dir(Path("job.qqinfo"), force=True, yes=True, job=None)


def test_wipe_invokes_repeater_and_exits_success(tmp_path):
    dummy_file = tmp_path / "info.qq"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()

    with (
        patch(
            "qq_lib.wipe.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
        patch("qq_lib.wipe.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.wipe.cli.logger"),
    ):
        result = runner.invoke(wipe, [])

        assert result.exit_code == 0

        calls = [call[0][0] for call in repeater_mock.onException.call_args_list]
        assert QQJobMismatchError in calls
        assert QQNotSuitableError in calls
        assert QQError in calls

        repeater_mock.run.assert_called_once()


def test_wipe_catches_qqerror_and_exits_91(tmp_path):
    dummy_file = tmp_path / "info.qq"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = QQError("wipe failure")

    with (
        patch(
            "qq_lib.wipe.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
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
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = Exception("unexpected wipe crash")

    with (
        patch(
            "qq_lib.wipe.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
        patch("qq_lib.wipe.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.wipe.cli.logger") as mock_logger,
    ):
        result = runner.invoke(wipe, [])

        assert result.exit_code == CFG.exit_codes.unexpected_error
        mock_logger.critical.assert_called_once_with(
            repeater_mock.run.side_effect, exc_info=True, stack_info=True
        )
