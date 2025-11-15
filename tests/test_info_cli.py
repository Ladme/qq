# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.info.cli import _info_for_job, info


def test_info_for_job_short_prints_short_info():
    informer_mock = MagicMock()
    presenter_mock = MagicMock()
    short_info_mock = MagicMock()

    presenter_mock.getShortInfo.return_value = short_info_mock

    with (
        patch(
            "qq_lib.info.cli.Presenter", return_value=presenter_mock
        ) as presenter_cls,
        patch("qq_lib.info.cli.Console") as console_cls,
    ):
        console_instance = console_cls.return_value
        _info_for_job(informer_mock, short=True)

        presenter_cls.assert_called_once_with(informer_mock)
        presenter_mock.getShortInfo.assert_called_once()
        console_instance.print.assert_called_once_with(short_info_mock)


def test_info_for_job_full_prints_full_info_panel():
    informer_mock = MagicMock()
    presenter_mock = MagicMock()
    panel_mock = MagicMock()

    presenter_mock.createFullInfoPanel.return_value = panel_mock

    with (
        patch(
            "qq_lib.info.cli.Presenter", return_value=presenter_mock
        ) as presenter_cls,
        patch("qq_lib.info.cli.Console") as console_cls,
    ):
        console_instance = console_cls.return_value
        _info_for_job(informer_mock, short=False)

        presenter_cls.assert_called_once_with(informer_mock)
        presenter_mock.createFullInfoPanel.assert_called_once_with(console_instance)
        console_instance.print.assert_called_once_with(panel_mock)


def test_info_invokes_repeater_and_exits_success(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    repeater_mock = MagicMock()
    informer_mock = MagicMock()

    runner = CliRunner()

    with (
        patch("qq_lib.info.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.info.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.info.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.info.cli.logger"),
    ):
        result = runner.invoke(info, [])

    assert result.exit_code == 0
    repeater_mock.run.assert_called_once()


def test_info_catches_qqerror_and_exits_91(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    repeater_mock = MagicMock()
    informer_mock = MagicMock()
    repeater_mock.run.side_effect = QQError("error occurred")

    runner = CliRunner()

    with (
        patch("qq_lib.info.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.info.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.info.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.info.cli.logger") as mock_logger,
    ):
        result = runner.invoke(info, [])

    assert result.exit_code == CFG.exit_codes.default
    mock_logger.error.assert_called_once_with(repeater_mock.run.side_effect)


def test_info_catches_generic_exception_and_exits_99(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy")

    repeater_mock = MagicMock()
    informer_mock = MagicMock()
    repeater_mock.run.side_effect = Exception("fatal error")

    runner = CliRunner()

    with (
        patch("qq_lib.info.cli.get_info_files", return_value=[dummy_file]),
        patch("qq_lib.info.cli.Informer.fromFile", return_value=informer_mock),
        patch("qq_lib.info.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.info.cli.logger") as mock_logger,
    ):
        result = runner.invoke(info, [])

    assert result.exit_code == CFG.exit_codes.unexpected_error
    mock_logger.critical.assert_called_once()


def test_info_invokes_repeater_with_job_id_and_exits_success():
    repeater_mock = MagicMock()
    informer_mock = MagicMock()

    runner = CliRunner()

    with (
        patch("qq_lib.info.cli.Informer.fromJobId", return_value=informer_mock),
        patch("qq_lib.info.cli.Repeater", return_value=repeater_mock),
        patch("qq_lib.info.cli.logger"),
    ):
        result = runner.invoke(info, ["12345"])

    assert result.exit_code == 0
    repeater_mock.run.assert_called_once()
