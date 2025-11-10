# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.submit import submit


def test_submit_successful(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    runner = CliRunner()

    submitter_mock = MagicMock()
    submitter_mock.getInputDir.return_value = tmp_path
    submitter_mock.continuesLoop.return_value = False
    submitter_mock.submit.return_value = "job123"

    factory_mock = MagicMock()
    factory_mock.makeSubmitter.return_value = submitter_mock

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch(
            "qq_lib.submit.cli.SubmitterFactory", return_value=factory_mock
        ) as mock_factory_class,
        patch("qq_lib.submit.cli.get_runtime_files", return_value=[]),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        result = runner.invoke(submit, [str(script)])

        assert result.exit_code == 0

        mock_factory_class.assert_called_once()
        factory_mock.makeSubmitter.assert_called_once()
        submitter_mock.submit.assert_called_once()
        info_messages = [call.args[0] for call in mock_logger.info.call_args_list]
        assert any("job123" in msg for msg in info_messages)


def test_submit_script_does_not_exist(tmp_path):
    runner = CliRunner()
    missing_script = tmp_path / "missing.sh"

    with patch("qq_lib.submit.cli.logger") as mock_logger:
        result = runner.invoke(submit, [str(missing_script)])

        assert result.exit_code == CFG.exit_codes.default
        error_messages = [call.args[0] for call in mock_logger.error.call_args_list]
        assert any("does not exist" in str(msg) for msg in error_messages)


def test_submit_detects_runtime_files_and_aborts(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    runner = CliRunner()
    submitter_mock = MagicMock()
    submitter_mock.getInputDir.return_value = tmp_path
    submitter_mock.continuesLoop.return_value = False

    factory_mock = MagicMock()
    factory_mock.makeSubmitter.return_value = submitter_mock

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch("qq_lib.submit.cli.SubmitterFactory", return_value=factory_mock),
        patch(
            "qq_lib.submit.cli.get_runtime_files",
            return_value=[tmp_path / "file.qqout"],
        ),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        result = runner.invoke(submit, [str(script)])

        assert result.exit_code == CFG.exit_codes.default
        error_messages = [call.args[0] for call in mock_logger.error.call_args_list]
        assert any("Submission aborted" in str(msg) for msg in error_messages)
        factory_mock.makeSubmitter.assert_called_once()
        submitter_mock.continuesLoop.assert_called_once()


def test_submit_continues_loop_even_with_runtime_files(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    runner = CliRunner()
    submitter_mock = MagicMock()
    submitter_mock.getInputDir.return_value = tmp_path
    submitter_mock.continuesLoop.return_value = True
    submitter_mock.submit.return_value = "job_loop"

    factory_mock = MagicMock()
    factory_mock.makeSubmitter.return_value = submitter_mock

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch("qq_lib.submit.cli.SubmitterFactory", return_value=factory_mock),
        patch(
            "qq_lib.submit.cli.get_runtime_files",
            return_value=[tmp_path / "file.qqout"],
        ),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        result = runner.invoke(submit, [str(script)])

        assert result.exit_code == 0
        info_messages = [call.args[0] for call in mock_logger.info.call_args_list]
        assert any("job_loop" in msg for msg in info_messages)
        factory_mock.makeSubmitter.assert_called_once()
        submitter_mock.submit.assert_called_once()
        submitter_mock.continuesLoop.assert_called_once()


def test_submit_generic_exception_results_in_critical_log(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    runner = CliRunner()

    factory_mock = MagicMock()
    factory_mock.makeSubmitter.side_effect = Exception("unexpected error")

    with (
        patch("qq_lib.submit.cli.Path.is_file", return_value=True),
        patch("qq_lib.submit.cli.SubmitterFactory", return_value=factory_mock),
        patch("qq_lib.submit.cli.logger") as mock_logger,
    ):
        result = runner.invoke(submit, [str(script)])

        assert result.exit_code == CFG.exit_codes.unexpected_error
        critical_messages = [
            call.args[0] for call in mock_logger.critical.call_args_list
        ]
        assert any("unexpected error" in str(msg) for msg in critical_messages)
        factory_mock.makeSubmitter.assert_called_once()
