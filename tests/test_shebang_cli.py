# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.shebang.cli import SHEBANG, _replaceOrAddShebang, shebang


def test_replace_or_add_shebang_raises_error_if_not_file(tmp_path):
    non_existent_file = tmp_path / "non_existent.sh"
    with pytest.raises(QQError):
        _replaceOrAddShebang(non_existent_file)


@pytest.mark.parametrize(
    "original_content",
    [
        ("#!/usr/bin/env infinity-env\necho hi\n"),
        ("#!/bin/bash\nx=1\n"),
    ],
)
def test_replace_or_add_shebang_replaces_existing_shebang(tmp_path, original_content):
    file_path = tmp_path / "script.sh"
    file_path.write_text(original_content)
    _replaceOrAddShebang(file_path)

    result = file_path.read_text().splitlines()
    assert result[0] == SHEBANG
    assert result[-1] != ""


@pytest.mark.parametrize(
    "original_content",
    [
        "print('hello')\n",
        "\nprint('x')\n",
        "",
    ],
)
def test_replace_or_add_shebang_adds_shebang_if_missing(tmp_path, original_content):
    file_path = tmp_path / "no_shebang"
    file_path.write_text(original_content)
    _replaceOrAddShebang(file_path)

    result = file_path.read_text()
    assert result.startswith(SHEBANG + "\n\n")


def test_shebang_prints_shebang_and_exits_success():
    runner = CliRunner()
    result = runner.invoke(shebang, [])
    assert result.exit_code == 0
    assert SHEBANG in result.output


def test_shebang_invokes_replace_or_add_shebang_and_exits_success(tmp_path):
    dummy_file = tmp_path / "script.sh"
    dummy_file.write_text("echo Hi")
    runner = CliRunner()
    with patch("qq_lib.shebang.cli._replaceOrAddShebang") as mock_replace:
        result = runner.invoke(shebang, [str(dummy_file)])
        assert result.exit_code == 0
        mock_replace.assert_called_once_with(Path(dummy_file))


def test_shebang_catches_qqerror_and_exits_91(tmp_path):
    dummy_file = tmp_path / "script.sh"
    dummy_file.write_text("x=1")
    runner = CliRunner()
    with (
        patch(
            "qq_lib.shebang.cli._replaceOrAddShebang", side_effect=QQError("failure")
        ),
        patch("qq_lib.shebang.cli.logger") as mock_logger,
    ):
        result = runner.invoke(shebang, [str(dummy_file)])
        assert result.exit_code == CFG.exit_codes.default
        mock_logger.error.assert_called_once()
        assert "failure" in str(mock_logger.error.call_args[0][0])


def test_shebang_catches_generic_exception_and_exits_99(tmp_path):
    dummy_file = tmp_path / "script.sh"
    dummy_file.write_text("echo test")
    runner = CliRunner()
    with (
        patch(
            "qq_lib.shebang.cli._replaceOrAddShebang",
            side_effect=Exception("unexpected error"),
        ),
        patch("qq_lib.shebang.cli.logger") as mock_logger,
    ):
        result = runner.invoke(shebang, [str(dummy_file)])
        assert result.exit_code == CFG.exit_codes.unexpected_error
        mock_logger.critical.assert_called_once_with(
            mock_logger.critical.call_args[0][0], exc_info=True, stack_info=True
        )
