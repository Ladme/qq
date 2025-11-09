# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQJobMismatchError
from qq_lib.info.cli import _info_for_job, info


def test_info_for_job_short_prints_short_info(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    informer_mock = MagicMock()
    presenter_mock = MagicMock()
    short_info_mock = MagicMock()

    informer_mock.isJob.return_value = True
    presenter_mock.getShortInfo.return_value = short_info_mock

    with (
        patch("qq_lib.info.cli.QQInformer.fromFile", return_value=informer_mock),
        patch(
            "qq_lib.info.cli.QQPresenter", return_value=presenter_mock
        ) as presenter_cls,
        patch("qq_lib.info.cli.Console") as console_cls,
    ):
        console_instance = console_cls.return_value
        _info_for_job(dummy_file, short=True, job=None)

        presenter_cls.assert_called_once_with(informer_mock)
        presenter_mock.getShortInfo.assert_called_once()
        console_instance.print.assert_called_once_with(short_info_mock)


def test_info_for_job_full_prints_full_info_panel(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    informer_mock = MagicMock()
    presenter_mock = MagicMock()
    panel_mock = MagicMock()

    informer_mock.isJob.return_value = True
    presenter_mock.createFullInfoPanel.return_value = panel_mock

    with (
        patch("qq_lib.info.cli.QQInformer.fromFile", return_value=informer_mock),
        patch(
            "qq_lib.info.cli.QQPresenter", return_value=presenter_mock
        ) as presenter_cls,
        patch("qq_lib.info.cli.Console") as console_cls,
    ):
        console_instance = console_cls.return_value
        _info_for_job(dummy_file, short=False, job=None)

        presenter_cls.assert_called_once_with(informer_mock)
        presenter_mock.createFullInfoPanel.assert_called_once_with(console_instance)
        console_instance.print.assert_called_once_with(panel_mock)


def test_info_for_job_raises_error_if_job_mismatch(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    informer_mock = MagicMock()
    informer_mock.matchesJob.return_value = False

    with (
        patch("qq_lib.info.cli.QQInformer.fromFile", return_value=informer_mock),
        pytest.raises(
            QQJobMismatchError,
            match="Info file for job 'job123' does not exist or is not reachable.",
        ),
    ):
        _info_for_job(dummy_file, short=True, job="job123")


def test_info_invokes_repeater_and_exits_success(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()

    with (
        patch(
            "qq_lib.info.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
        patch("qq_lib.info.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.info.cli.logger"),
    ):
        result = runner.invoke(info, [])

        assert result.exit_code == 0
        repeater_mock.run.assert_called_once()


def test_info_catches_qqerror_and_exits_91(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = QQError("error occurred")

    with (
        patch(
            "qq_lib.info.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
        patch("qq_lib.info.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.info.cli.logger") as mock_logger,
    ):
        result = runner.invoke(info, [])

        assert result.exit_code == CFG.exit_codes.default
        mock_logger.error.assert_called_once_with(repeater_mock.run.side_effect)


def test_info_catches_generic_exception_and_exits_99(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    runner = CliRunner()
    repeater_mock = MagicMock()
    repeater_mock.run.side_effect = Exception("fatal error")

    with (
        patch(
            "qq_lib.info.cli.get_info_files_from_job_id_or_dir",
            return_value=[dummy_file],
        ),
        patch("qq_lib.info.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.info.cli.logger") as mock_logger,
    ):
        result = runner.invoke(info, [])

        assert result.exit_code == CFG.exit_codes.unexpected_error
        mock_logger.critical.assert_called_once()
