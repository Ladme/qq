# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from qq_lib.common import (
    convert_to_batch_system,
    get_files_with_suffix,
    get_info_file,
    yes_or_no_prompt,
)
from qq_lib.error import QQError
from qq_lib.pbs import QQPBS


def test_no_files_with_matching_suffix():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        # files not matching the suffix
        (tmp_path / "file1.txt").write_text("hello")
        (tmp_path / "file2.doc").write_text("world")

        result = get_files_with_suffix(tmp_path, ".qqinfo")
        assert result == []


def test_multiple_files_with_matching_suffix():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        matching_files = []
        for i in range(3):
            file = tmp_path / f"file{i}.qqout"
            file.write_text(f"content {i}")
            matching_files.append(file)

        (tmp_path / "other.out").write_text("ignore me")

        result = get_files_with_suffix(tmp_path, ".qqout")

        assert sorted(result) == sorted(matching_files)


def test_no_info_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        with pytest.raises(QQError, match="No qq job info file found."):
            get_info_file(tmp_path)


def test_one_info_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        file = tmp_path / "job.qqinfo"
        file.write_text("some info")

        result = get_info_file(tmp_path)
        assert result == file


def test_multiple_info_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        file1 = tmp_path / "job1.qqinfo"
        file1.write_text("info1")
        file2 = tmp_path / "job2.qqinfo"
        file2.write_text("info2")

        with pytest.raises(QQError, match="Multiple"):
            get_info_file(tmp_path)


def test_yes_key():
    with patch("readchar.readkey", return_value="y"):
        result = yes_or_no_prompt("Do you agree?")
        assert result is True


def test_no_key():
    with patch("readchar.readkey", return_value="n"):
        result = yes_or_no_prompt("Do you agree?")
        assert result is False


def test_other_key():
    with patch("readchar.readkey", return_value="x"):
        result = yes_or_no_prompt("Do you agree?")
        assert result is False


def test_valid_batch_system_name():
    name = QQPBS.envName()
    result = convert_to_batch_system(name)
    assert result is QQPBS


def test_invalid_batch_system_name():
    with pytest.raises(KeyError):
        convert_to_batch_system("FakePBS")
