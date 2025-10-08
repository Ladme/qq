# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
import tempfile
from datetime import timedelta
from pathlib import Path
from time import sleep
from unittest.mock import patch

import pytest

from qq_lib.core.common import (
    convert_absolute_to_relative,
    equals_normalized,
    format_duration,
    format_duration_wdhhmmss,
    get_files_with_suffix,
    get_info_file,
    get_info_files,
    hhmmss_to_duration,
    hhmmss_to_wdhms,
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


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("0:00:00", "0s"),
        ("0:00:09", "9s"),
        ("0:03:00", "3m"),
        ("1:05:07", "1h5m7s"),
        ("10:00:00", "10h"),
        ("24:00:00", "1d"),
        ("168:00:00", "1w"),
        ("195:04:05", "1w1d3h4m5s"),
        ("219:04:05", "1w2d3h4m5s"),
        ("72:12:02", "3d12m2s"),
        ("170:00:48", "1w2h48s"),
        ("1:30:00", "1h30m"),
        ("49:00:00", "2d1h"),
        ("168:00:11", "1w11s"),
        ("0:00:00", "0s"),
    ],
)
def test_hhmmss_to_wdhms(input_str, expected):
    assert hhmmss_to_wdhms(input_str) == expected


@pytest.mark.parametrize(
    "invalid_input",
    [
        "1:60:00",
        "1:00:60",
        "abc",
        "1:2",
        "1::2",
        "24:00:00:00",
        "1:2:3:4",
        "1:2:60",
        "-1:00:00",
        "1:-2:00",
        "1:00:-5",
    ],
)
def test_hhmmss_to_wdhms_invalid_strings(invalid_input):
    with pytest.raises(QQError):
        hhmmss_to_wdhms(invalid_input)


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("0:00:00", "0s"),
        ("0:00:01", "1s"),
        ("0:01:00", "1m"),
        ("1:00:00", "1h"),
        ("24:00:00", "1d"),
        ("168:00:00", "1w"),
        ("192:30:15", "1w1d30m15s"),
    ],
)
def test_hhmmss_to_wdhms_edge_cases(input_str, expected):
    assert hhmmss_to_wdhms(input_str) == expected


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("49:00:00", "2d1h"),
        ("90:00:00", "3d18h"),
        ("1:30:00", "1h30m"),
        ("3600:00:00", "21w3d"),
    ],
)
def test_hhmmss_to_wdhms_large_values(input_str, expected):
    assert hhmmss_to_wdhms(input_str) == expected


@pytest.mark.parametrize(
    "input_str,expected",
    [
        # seconds only
        ("45s", "0:00:45"),
        ("5s", "0:00:05"),
        # minutes only
        ("3m", "0:03:00"),
        # hours only
        ("10h", "10:00:00"),
        # days and weeks only
        ("1d", "24:00:00"),
        ("1w", "168:00:00"),
        # combined compact and space-separated
        ("1w2d3h4m5s", "219:04:05"),
        ("1w  2d 3h 4m 5s", "219:04:05"),
        ("1w  2d3h   4m5s", "219:04:05"),
        # case insensitive units
        ("1W 2D 3H 4M 5S", "219:04:05"),
        ("1w 2D 3h 4M 5s", "219:04:05"),
        # padding
        ("1h 5m 7s", "1:05:07"),
        ("0h 0m 9s", "0:00:09"),
        # skipped values
        ("1d 1s", "24:00:01"),
        ("3d 12m 2s", "72:12:02"),
        ("1w 2h 48s", "170:00:48"),
        # empty or whitespace treated as zero
        ("", "0:00:00"),
        ("   ", "0:00:00"),
        # multiple same units accumulate
        ("1h 2h 30m", "3:30:00"),
        ("1d 24h", "48:00:00"),
        # large values and rollover
        ("90m", "1:30:00"),
        ("3600s", "1:00:00"),
        ("1w 90m 3666s", "170:31:06"),
        # zero values
        ("0h 0m 0s", "0:00:00"),
        ("0w 0d", "0:00:00"),
    ],
)
def test_wdhms_to_hhmmss_valid(input_str, expected):
    assert wdhms_to_hhmmss(input_str) == expected


@pytest.mark.parametrize(
    "invalid_input",
    [
        "1h abc 2m",
        "foo",
        "1h2x",
        "1.5h",
        "0.5m",
    ],
)
def test_wdhms_to_hhmmss_invalid(invalid_input):
    with pytest.raises(QQError):
        wdhms_to_hhmmss(invalid_input)


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


@pytest.mark.parametrize(
    "input_td,expected",
    [
        (timedelta(seconds=0), "0s"),
        (timedelta(seconds=45), "45s"),
        (timedelta(minutes=3), "3m"),
        (timedelta(hours=10), "10h"),
        (timedelta(days=1), "1d"),
        (timedelta(weeks=1), "1w"),
        (timedelta(minutes=1, seconds=5), "1m 5s"),
        (timedelta(hours=2, minutes=30), "2h 30m"),
        (timedelta(days=1, hours=2), "1d 2h"),
        (timedelta(days=8, hours=3), "1w 1d 3h"),
        (timedelta(weeks=2, days=1, hours=3, minutes=4, seconds=5), "2w 1d 3h 4m 5s"),
        (timedelta(days=15, hours=26, minutes=61, seconds=3661), "2w 2d 4h 2m 1s"),
        (timedelta(minutes=0, seconds=59), "59s"),
        (timedelta(minutes=1, seconds=0), "1m"),
        (timedelta(hours=5, seconds=30), "5h 30s"),
        (timedelta(days=1, minutes=1, seconds=1), "1d 1m 1s"),
        (timedelta(days=7), "1w"),
        (timedelta(days=14), "2w"),
        (timedelta(days=7, hours=1), "1w 1h"),
        (timedelta(days=15, minutes=10), "2w 1d 10m"),
    ],
)
def test_format_duration_valid(input_td, expected):
    assert format_duration(input_td) == expected


@pytest.mark.parametrize(
    "input_td,expected_str",
    [
        (timedelta(seconds=3600), "1h"),
        (timedelta(seconds=3661), "1h 1m 1s"),
        (timedelta(seconds=86400), "1d"),
        (timedelta(seconds=90061), "1d 1h 1m 1s"),
        (timedelta(seconds=604800), "1w"),
        (timedelta(seconds=691200), "1w 1d"),
    ],
)
def test_format_duration_rollover_boundaries(input_td, expected_str):
    assert format_duration(input_td) == expected_str


@pytest.mark.parametrize(
    "td,expected",
    [
        (timedelta(seconds=0), "00:00:00"),
        (timedelta(seconds=5), "00:00:05"),
        (timedelta(seconds=45), "00:00:45"),
        (timedelta(minutes=3), "00:03:00"),
        (timedelta(minutes=90), "01:30:00"),
        (timedelta(hours=1), "01:00:00"),
        (timedelta(hours=10), "10:00:00"),
        (timedelta(hours=36), "1d 12:00:00"),
        (timedelta(days=1), "1d 00:00:00"),
        (timedelta(days=3), "3d 00:00:00"),
        (timedelta(days=10), "1w 3d 00:00:00"),
        (timedelta(days=14), "2w 00:00:00"),
        (timedelta(days=21), "3w 00:00:00"),
        (timedelta(days=1, hours=2, minutes=3, seconds=4), "1d 02:03:04"),
        (timedelta(days=10, hours=5, minutes=6, seconds=7), "1w 3d 05:06:07"),
        (timedelta(weeks=1, days=2, hours=3, minutes=4, seconds=5), "1w 2d 03:04:05"),
        (timedelta(weeks=2, days=0, hours=12, minutes=30, seconds=15), "2w 12:30:15"),
        (timedelta(days=7), "1w 00:00:00"),
        (timedelta(days=14, hours=1), "2w 01:00:00"),
        (timedelta(hours=5, minutes=7, seconds=9), "05:07:09"),
        (timedelta(days=0, hours=0, minutes=0, seconds=1), "00:00:01"),
        (timedelta(days=0, hours=0, minutes=1, seconds=0), "00:01:00"),
        (timedelta(days=0, hours=1, minutes=0, seconds=0), "01:00:00"),
    ],
)
def test_format_duration_wdhhmmss(td, expected):
    assert format_duration_wdhhmmss(td) == expected


@pytest.mark.parametrize(
    "timestr, expected",
    [
        ("00:00:00", timedelta(0)),
        ("0:00:00", timedelta(0)),
        ("0000:00:00", timedelta(0)),
        ("00:00:01", timedelta(seconds=1)),
        ("00:01:00", timedelta(minutes=1)),
        ("01:00:00", timedelta(hours=1)),
        ("12:34:56", timedelta(hours=12, minutes=34, seconds=56)),
        ("36:15:00", timedelta(hours=36, minutes=15)),
        ("100:00:05", timedelta(hours=100, seconds=5)),
        ("999:59:59", timedelta(hours=999, minutes=59, seconds=59)),
        ("1:2:3", timedelta(hours=1, minutes=2, seconds=3)),
        ("36:15:00 ", timedelta(hours=36, minutes=15)),
        (" 36:15:00", timedelta(hours=36, minutes=15)),
        (" 36:15:00 ", timedelta(hours=36, minutes=15)),
    ],
)
def test_hhmmss_to_duration_valid(timestr, expected):
    assert hhmmss_to_duration(timestr) == expected


@pytest.mark.parametrize(
    "timestr",
    [
        "24:00",
        "12:34:56:78",
        "12-34-56",
        "abc",
        "1h23m45s",
        "",
        "   ",
    ],
)
def test_hhmmss_to_duration_invalid_format(timestr):
    with pytest.raises(QQError):
        hhmmss_to_duration(timestr)


@pytest.mark.parametrize(
    "timestr",
    [
        "12:60:00",
        "12:00:60",
        "00:99:99",
    ],
)
def test_hhmmss_to_duration_invalid_ranges(timestr):
    with pytest.raises(QQError):
        hhmmss_to_duration(timestr)
