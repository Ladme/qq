# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner

from qq_lib.clear.cli import QQClearer, clear
from qq_lib.core.constants import (
    QQ_INFO_SUFFIX,
    QQ_OUT_SUFFIX,
    STDERR_SUFFIX,
    STDOUT_SUFFIX,
)
from qq_lib.core.error import QQError
from qq_lib.info.informer import QQInformer
from qq_lib.properties.states import RealState


def test_qqclearer_init_sets_directory():
    dummy_path = Path("/fake/path")
    clearer = QQClearer(dummy_path)
    assert clearer._directory == dummy_path


def test_qqclearer_delete_files_deletes_all_files():
    mock_file1 = Mock(spec=Path)
    mock_file2 = Mock(spec=Path)

    QQClearer._deleteFiles([mock_file1, mock_file2])

    mock_file1.unlink.assert_called_once()
    mock_file2.unlink.assert_called_once()


def test_qq_clearer_collect_run_time_files_returns_files_from_helper(tmp_path):
    clearer = QQClearer(tmp_path)
    expected_files = [tmp_path / f"a{QQ_INFO_SUFFIX}", tmp_path / f"b{QQ_OUT_SUFFIX}"]

    with patch(
        "qq_lib.clear.clearer.get_runtime_files", return_value=expected_files
    ) as mock_get:
        result = clearer._collectRunTimeFiles()

        mock_get.assert_called_once_with(tmp_path)
        assert result == set(expected_files)


@pytest.mark.parametrize("state", list(RealState))
def test_qq_clearer_collect_excluded_files(tmp_path, state):
    clearer = QQClearer(tmp_path)
    dummy_info_file = tmp_path / f"job{QQ_INFO_SUFFIX}"
    dummy_info_file.touch()

    dummy_stdout = f"stdout{STDOUT_SUFFIX}"
    dummy_stderr = f"stderr{STDERR_SUFFIX}"
    dummy_job_name = "job"

    mock_informer = MagicMock()
    mock_informer.getRealState.return_value = state
    mock_informer.info.stdout_file = dummy_stdout
    mock_informer.info.stderr_file = dummy_stderr
    mock_informer.info.job_name = dummy_job_name

    with (
        patch("qq_lib.core.common.get_info_files", return_value=[dummy_info_file]),
        patch("qq_lib.info.informer.QQInformer.fromFile", return_value=mock_informer),
    ):
        result = clearer._collectExcludedFiles()

    if state in [
        RealState.KILLED,
        RealState.FAILED,
        RealState.IN_AN_INCONSISTENT_STATE,
    ]:
        assert dummy_info_file not in result
        assert tmp_path / dummy_stdout
        assert (tmp_path / dummy_job_name).with_suffix(QQ_OUT_SUFFIX)
    else:
        expected_files = {
            dummy_info_file,
            tmp_path / dummy_stdout,
            tmp_path / dummy_stderr,
            (tmp_path / dummy_job_name).with_suffix(QQ_OUT_SUFFIX),
        }
        assert result == expected_files


def test_qq_clearer_collect_excluded_files_ignores_files_that_raise_qqerror(tmp_path):
    clearer = QQClearer(tmp_path)
    dummy_info_file = tmp_path / f"bad{QQ_INFO_SUFFIX}"
    dummy_info_file.touch()

    with (
        patch("qq_lib.core.common.get_info_files", return_value=[dummy_info_file]),
        patch.object(QQInformer, "fromFile", side_effect=QQError("cannot read file")),
    ):
        result = clearer._collectExcludedFiles()

    assert result == set()


def test_qq_clearer_clear_deletes_only_safe_files(tmp_path):
    clearer = QQClearer(tmp_path)

    safe_file = tmp_path / f"safe{QQ_OUT_SUFFIX}"
    excluded_file = tmp_path / f"excluded{QQ_OUT_SUFFIX}"

    with (
        patch.object(
            QQClearer, "_collectRunTimeFiles", return_value={safe_file, excluded_file}
        ),
        patch.object(QQClearer, "_collectExcludedFiles", return_value={excluded_file}),
        patch.object(QQClearer, "_deleteFiles") as mock_delete,
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear()

        mock_delete.assert_called_once_with({safe_file})

        messages = [call.args[0] for call in mock_info.call_args_list]
        assert any("Removed" in msg and "qq file" in msg for msg in messages)
        assert any("not safe to clear" in msg for msg in messages)


def test_qq_clearer_clear_deletes_no_files_are_safe(tmp_path):
    clearer = QQClearer(tmp_path)

    excluded1 = tmp_path / f"excluded1{QQ_OUT_SUFFIX}"
    excluded2 = tmp_path / f"excluded2{QQ_OUT_SUFFIX}"

    with (
        patch.object(
            QQClearer, "_collectRunTimeFiles", return_value={excluded1, excluded2}
        ),
        patch.object(
            QQClearer, "_collectExcludedFiles", return_value={excluded1, excluded2}
        ),
        patch.object(QQClearer, "_deleteFiles") as mock_delete,
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear()

        mock_delete.assert_not_called()

        messages = [call.args[0] for call in mock_info.call_args_list]
        assert any("No qq files could be safely cleared" in msg for msg in messages)


def test_qq_clearer_clear_force_deletes_all_files(tmp_path):
    clearer = QQClearer(tmp_path)

    file1 = tmp_path / f"file1{QQ_OUT_SUFFIX}"
    file2 = tmp_path / f"file2{QQ_OUT_SUFFIX}"

    with (
        patch.object(QQClearer, "_collectRunTimeFiles", return_value={file1, file2}),
        patch.object(QQClearer, "_collectExcludedFiles") as mock_excluded,
        patch.object(QQClearer, "_deleteFiles") as mock_delete,
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear(force=True)

        mock_excluded.assert_not_called()
        mock_delete.assert_called_once_with({file1, file2})

        messages = [call.args[0] for call in mock_info.call_args_list]
        assert any("Removed" in msg and "qq file" in msg for msg in messages)


def test_qq_clearer_clear_logs_info_when_no_files(tmp_path):
    clearer = QQClearer(tmp_path)

    with (
        patch.object(QQClearer, "_collectRunTimeFiles", return_value=set()),
        patch.object(QQClearer, "_deleteFiles") as mock_delete,
        patch("qq_lib.clear.clearer.logger.info") as mock_info,
    ):
        clearer.clear()

        mock_delete.assert_not_called()

        messages = [call.args[0] for call in mock_info.call_args_list]
        assert any("Nothing to clear" in msg for msg in messages)


def test_clear_command_runs_successfully():
    runner = CliRunner()
    dummy_clear = patch.object(QQClearer, "clear")

    with dummy_clear as mock_clear:
        result = runner.invoke(clear, [])
        assert result.exit_code == 0
        mock_clear.assert_called_once_with(False)


def test_clear_command_with_force_flag():
    runner = CliRunner()
    dummy_clear = patch.object(QQClearer, "clear")

    with dummy_clear as mock_clear:
        result = runner.invoke(clear, ["--force"])
        assert result.exit_code == 0
        mock_clear.assert_called_once_with(True)


def test_clear_command_qqerror_triggers_exit_91():
    runner = CliRunner()

    def raise_qqerror(force):
        _ = force
        raise QQError("some error")

    with patch.object(QQClearer, "clear", side_effect=raise_qqerror):
        result = runner.invoke(clear, [])
        assert result.exit_code == 91
        assert "some error" in result.output


def test_clear_command_unexpected_exception_triggers_exit_99():
    runner = CliRunner()

    def raise_exception(force):
        _ = force
        raise RuntimeError("unexpected")

    with patch.object(QQClearer, "clear", side_effect=raise_exception):
        result = runner.invoke(clear, [])
        assert result.exit_code == 99
        assert "unexpected" in result.output
