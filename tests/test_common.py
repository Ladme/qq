# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from qq_lib.common import (
    convert_absolute_to_relative,
    equals_normalized,
    get_files_with_suffix,
    get_info_file,
    yes_or_no_prompt,
)
from qq_lib.error import QQError


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


@pytest.mark.parametrize(
    "a, b",
    [
        ("hello", "hello"),
        ("Hello", "hello"),
        ("WORLD", "world"),
        ("hello-world", "helloworld"),
        ("a-b-c", "abc"),
        ("hello_world", "helloworld"),
        ("a_b_c", "abc"),
        ("Hello-World_test", "helloworldtest"),
        ("", ""),
    ],
)
def test_equals_normalized_true(a, b):
    assert equals_normalized(a, b) is True


@pytest.mark.parametrize(
    "a, b",
    [
        ("hello", "world"),
        ("hello_world", "hello-worldx"),
        ("", "nonempty"),
    ],
)
def test_equals_normalized_false(a, b):
    assert equals_normalized(a, b) is False


def test_convert_absolute_to_relative_success(tmp_path):
    target = tmp_path
    file1 = target / "a.txt"
    file2 = target / "subdir" / "b.txt"
    file2.parent.mkdir()
    file1.write_text("data1")
    file2.write_text("data2")

    result = convert_absolute_to_relative([file1, file2], target)

    assert result == [Path("a.txt"), Path("subdir") / "b.txt"]


def test_convert_absolute_to_relative_file_outside_target(tmp_path):
    target = tmp_path / "target"
    outside = tmp_path / "outside.txt"
    target.mkdir()
    outside.write_text("oops")

    with pytest.raises(QQError, match="is not in target directory"):
        convert_absolute_to_relative([outside], target)


def test_convert_absolute_to_relative_empty_list(tmp_path):
    target = tmp_path
    result = convert_absolute_to_relative([], target)
    assert result == []


def test_convert_absolute_to_relative_mixed_inside_and_outside(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    inside = target / "file.txt"
    inside.write_text("inside")
    outside = tmp_path / "outside.txt"
    outside.write_text("outside")

    with pytest.raises(QQError):
        convert_absolute_to_relative([inside, outside], target)
