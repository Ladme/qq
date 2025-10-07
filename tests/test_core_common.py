# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
import tempfile
from pathlib import Path
from time import sleep
from unittest.mock import patch

import pytest

from qq_lib.core.common import (
    convert_absolute_to_relative,
    equals_normalized,
    get_files_with_suffix,
    get_info_file,
    get_info_files,
    is_printf_pattern,
    printf_to_regex,
    split_files_list,
    to_snake_case,
    wdhms_to_hhmmss,
    yes_or_no_prompt,
)
from qq_lib.core.error import QQError


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


def test_get_info_file_no_info_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        with pytest.raises(QQError, match="No qq job info file found."):
            get_info_file(tmp_path)


def test_get_info_file_one_info_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        file = tmp_path / "job.qqinfo"
        file.write_text("some info")

        result = get_info_file(tmp_path)
        assert result == file


def test_get_info_file_multiple_info_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        file1 = tmp_path / "job1.qqinfo"
        file1.write_text("info1")
        file2 = tmp_path / "job2.qqinfo"
        file2.write_text("info2")

        with pytest.raises(QQError, match="Multiple"):
            get_info_file(tmp_path)


def test_get_info_file_no_info_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        result = get_info_files(tmp_path)
        assert result == []


def test_get_info_file_single_info_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        file1 = tmp_path / "job1.qqinfo"
        file1.write_text("info1")

        result = get_info_files(tmp_path)
        assert result == [file1]


def test_get_info_files_multiple_info_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        file1 = tmp_path / "job1.qqinfo"
        file1.write_text("info1")
        file2 = tmp_path / "job2.qqinfo"
        file2.write_text("info2")

        result = get_info_files(tmp_path)
        assert result == [file1, file2]


def test_get_info_files_ignore_non_info_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        qq_file = tmp_path / "job1.qqinfo"
        qq_file.write_text("info1")
        other_file = tmp_path / "readme.txt"
        other_file.write_text("not info")

        result = get_info_files(tmp_path)
        assert result == [qq_file]


def test_get_info_files_info_files_in_subdirectories_not_included():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        sub_dir = tmp_path / "sub"
        sub_dir.mkdir()
        file_in_sub = sub_dir / "job1.qqinfo"
        file_in_sub.write_text("info1")

        result = get_info_files(tmp_path)
        assert result == []


def test_get_info_files_sorted(tmp_path):
    file1 = tmp_path / "job1.qqinfo"
    file2 = tmp_path / "job2.qqinfo"
    file3 = tmp_path / "job3.qqinfo"

    file3.write_text("one")
    sleep(0.1)
    file2.write_text("two")
    sleep(0.1)
    file1.write_text("three")

    result = get_info_files(tmp_path)

    assert result == [file3, file2, file1]


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


def test_wdhms_to_hhmmss_seconds_only():
    assert wdhms_to_hhmmss("45s") == "0:00:45"
    assert wdhms_to_hhmmss("5s") == "0:00:05"


def test_wdhms_to_hhmmss_minutes_only():
    assert wdhms_to_hhmmss("3m") == "0:03:00"


def test_wdhms_to_hhmmss_hours_only():
    assert wdhms_to_hhmmss("10h") == "10:00:00"


def test_wdhms_to_hhmmss_days_and_weeks_only():
    assert wdhms_to_hhmmss("1d") == "24:00:00"
    assert wdhms_to_hhmmss("1w") == "168:00:00"


def test_wdhms_to_hhmmss_combined_compact_and_spaces():
    assert wdhms_to_hhmmss("1w2d3h4m5s") == "219:04:05"
    assert wdhms_to_hhmmss("1w  2d 3h 4m 5s") == "219:04:05"
    assert wdhms_to_hhmmss("1w  2d3h   4m5s") == "219:04:05"


def test_wdhms_to_hhmmss_case_insensitive_units():
    assert wdhms_to_hhmmss("1W 2D 3H 4M 5S") == "219:04:05"
    assert wdhms_to_hhmmss("1w 2D 3h 4M 5s") == "219:04:05"


def test_wdhms_to_hhmmss_padding():
    assert wdhms_to_hhmmss("1h 5m 7s") == "1:05:07"
    assert wdhms_to_hhmmss("0h 0m 9s") == "0:00:09"


def test_wdhms_to_hhmmss_skipped_values():
    assert wdhms_to_hhmmss("1d 1s") == "24:00:01"
    assert wdhms_to_hhmmss("3d 12m 2s") == "72:12:02"
    assert wdhms_to_hhmmss("1w 2h 48s") == "170:00:48"


def test_wdhms_to_hhmmss_empty_or_whitespace_returns_zero():
    assert wdhms_to_hhmmss("") == "0:00:00"
    assert wdhms_to_hhmmss("   ") == "0:00:00"


def test_wdhms_to_hhmmss_invalid_characters_raise():
    with pytest.raises(QQError):
        wdhms_to_hhmmss("1h abc 2m")

    with pytest.raises(QQError):
        wdhms_to_hhmmss("foo")

    with pytest.raises(QQError):
        wdhms_to_hhmmss("1h2x")


def test_wdhms_to_hhmmss_decimal_values_raise():
    with pytest.raises(QQError):
        wdhms_to_hhmmss("1.5h")
    with pytest.raises(QQError):
        wdhms_to_hhmmss("0.5m")


def test_wdhms_to_hhmmss_multiple_same_units_accumulate():
    assert wdhms_to_hhmmss("1h 2h 30m") == "3:30:00"
    assert wdhms_to_hhmmss("1d 24h") == "48:00:00"


def test_wdhms_to_hhmmss_large_values_and_rollover():
    assert wdhms_to_hhmmss("90m") == "1:30:00"
    assert wdhms_to_hhmmss("3600s") == "1:00:00"
    assert wdhms_to_hhmmss("1w 90m 3666s") == "170:31:06"


def test_wdhms_to_hhmmss_zero_values_ok():
    assert wdhms_to_hhmmss("0h 0m 0s") == "0:00:00"
    assert wdhms_to_hhmmss("0w 0d") == "0:00:00"


@pytest.mark.parametrize(
    "pattern, test_string, should_match",
    [
        # simple zero-padded
        ("md%04d", "md0001", True),
        ("md%04d", "md1234", True),
        ("md%04d", "md123", False),
        ("md%04d", "md12345", False),
        # simple non-padded
        ("file%d", "file1", True),
        ("file%d", "file12345", True),
        ("file%d", "file", False),
        ("file%d", "file12a", False),
        # multiple placeholders
        ("file%03d_part%02d", "file001_part01", True),
        ("file%03d_part%02d", "file123_part99", True),
        ("file%03d_part%02d", "file12_part01", False),
        ("file%03d_part%02d", "file123_part1", False),
        # literal characters
        ("data(%d).txt", "data(12).txt", True),
        ("data(%d).txt", "data12.txt", False),
        # no placeholders
        ("readme.txt", "readme.txt", True),
        ("readme.txt", "readme1.txt", False),
        # adjacent placeholders
        ("%02d%03d", "01123", True),
        ("%02d%03d", "123", False),
    ],
)
def test_printf_to_regex(pattern, test_string, should_match):
    regex = printf_to_regex(pattern)
    match = re.fullmatch(regex, test_string) is not None
    assert match == should_match


@pytest.mark.parametrize(
    "pattern, expected_regex",
    [
        ("md%04d", r"^md\d{4}$"),
        ("file%d", r"^file\d+$"),
        ("file%03d_part%02d", r"^file\d{3}_part\d{2}$"),
        ("data(%d).txt", r"^data\(\d+\)\.txt$"),
        ("readme.txt", r"^readme\.txt$"),
        ("%02d%03d", r"^\d{2}\d{3}$"),
    ],
)
def test_regex_generation(pattern, expected_regex):
    assert printf_to_regex(pattern) == expected_regex


@pytest.mark.parametrize(
    "pattern, expected",
    [
        # simple cases
        ("md%04d", True),
        ("file%d", True),
        ("file%03d_part%02d", True),
        # no placeholders
        ("readme.txt", False),
        ("data_123.txt", False),
        ("md\\d{4}", False),
        # mixed text
        ("prefix%05d_suffix", True),
        ("start%0dend", True),
        ("%d", True),
        ("%0d", True),
        ("%", False),
        ("%05", False),
        ("%x", False),
    ],
)
def test_is_printf_pattern(pattern, expected):
    assert is_printf_pattern(pattern) == expected


def test_split_files_list_none_or_empty():
    # None input
    assert split_files_list(None) == []
    # empty string
    assert split_files_list("") == []


def test_split_files_list_whitespace(tmp_path):
    string = (
        f"{tmp_path / 'file1.txt'} {tmp_path / 'file2.txt'}\t{tmp_path / 'file3.txt'}"
    )
    expected = [
        Path(tmp_path / "file1.txt").resolve(),
        Path(tmp_path / "file2.txt").resolve(),
        Path(tmp_path / "file3.txt").resolve(),
    ]
    assert split_files_list(string) == expected


def test_split_files_list_commas_and_colons(tmp_path):
    string = (
        f"{tmp_path / 'file1.txt'},{tmp_path / 'file2.txt'}:{tmp_path / 'file3.txt'}"
    )
    expected = [
        Path(tmp_path / "file1.txt").resolve(),
        Path(tmp_path / "file2.txt").resolve(),
        Path(tmp_path / "file3.txt").resolve(),
    ]
    assert split_files_list(string) == expected


def test_split_files_list_mixed_separators(tmp_path):
    string = f"{tmp_path / 'file1.txt'}, {tmp_path / 'file2.txt'}:{tmp_path / 'file3.txt'} {tmp_path / 'file4.txt'}"
    expected = [
        Path(tmp_path / "file1.txt").resolve(),
        Path(tmp_path / "file2.txt").resolve(),
        Path(tmp_path / "file3.txt").resolve(),
        Path(tmp_path / "file4.txt").resolve(),
    ]
    assert split_files_list(string) == expected


def test_split_files_list_single_file(tmp_path):
    string = str(tmp_path / "single_file.txt")
    expected = [Path(tmp_path / "single_file.txt").resolve()]
    assert split_files_list(string) == expected


@pytest.mark.parametrize(
    "input_str,expected",
    [
        # PascalCase
        ("PascalCase", "pascal_case"),
        ("SimpleTest", "simple_test"),
        ("JSONParser", "j_s_o_n_parser"),
        ("HTTPRequest", "h_t_t_p_request"),
        # kebab-case
        ("kebab-case", "kebab_case"),
        ("multi-part-string", "multi_part_string"),
        # already snake_case
        ("already_snake_case", "already_snake_case"),
        ("singleword", "singleword"),
        # mixed PascalCase and kebab-case
        ("PascalCase-with-kebab", "pascal_case_with_kebab"),
        # edge cases
        ("", ""),  # empty string
        ("A", "a"),  # single capital letter
        ("a", "a"),  # single lowercase letter
        ("UPPERCASE", "u_p_p_e_r_c_a_s_e"),  # all capitals
    ],
)
def test_to_snake_case(input_str, expected):
    assert to_snake_case(input_str) == expected
