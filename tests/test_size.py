# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import pytest

from qq_lib.error import QQError
from qq_lib.size import Size  # assuming Size is defined in size.py


def test_init_and_str_repr():
    s = Size(10, "mb")
    assert str(s) == "10mb"
    assert repr(s) == "Size(value=10, unit='mb')"


def test_invalid_unit_raises():
    with pytest.raises(QQError):
        Size(5, "tb")


@pytest.mark.parametrize(
    "text, expected",
    [
        ("10mb", Size(10, "mb")),
        ("10 mb", Size(10, "mb")),
        ("2048KB", Size(2, "mb")),
    ],
)
def test_from_string_valid(text, expected):
    assert Size.from_string(text) == expected


def test_from_string_invalid():
    with pytest.raises(QQError):
        Size.from_string("nonsense")


@pytest.mark.parametrize(
    "value, unit, expected_value, expected_unit",
    [
        (2048, "kb", 2, "mb"),  # converts KB -> MB
        (1025, "kb", 2, "mb"),  # rounds up after conversion
        (1536, "kb", 2, "mb"),  # KB -> MB, rounding
        (1, "mb", 1, "mb"),  # stays MB, value >= 1
        (1024, "mb", 1, "gb"),  # MB -> GB
        (1536, "mb", 2, "gb"),  # MB -> GB, rounding
        (1, "kb", 1, "kb"),  # stays KB
        (0, "kb", 1, "kb"),  # below 1 KB defaults to 1 kb
        (2048, "mb", 2, "gb"),  # MB -> GB
        (1048576, "kb", 1, "gb"),  # KB -> GB
        (1048577, "kb", 2, "gb"),  # KB -> GB, rounding
    ],
)
def test_post_init_conversions(value, unit, expected_value, expected_unit):
    s = Size(value, unit)
    assert s.value == expected_value
    assert s.unit == expected_unit


def test_to_kb():
    assert Size(1, "kb").to_kb() == 1
    assert Size(1, "mb").to_kb() == 1024
    assert Size(1, "gb").to_kb() == 1024 * 1024


def test_multiplication():
    s = Size(2, "mb")
    result = s * 3
    assert result == Size(6, "mb")


def test_multiplication_large():
    s = Size(2, "mb")
    result = s * 1200
    assert result == Size(2400, "mb")


def test_reverse_multiplication():
    s = Size(4, "kb")
    result = 3 * s
    assert result == Size(12, "kb")


def test_floordiv_by_integer_basic():
    s = Size(10, "mb")
    result = s // 2
    assert result == Size(5, "mb")


def test_floordiv_by_integer_unit_conversion():
    s = Size(1, "gb")
    result = s // 8
    assert result == Size(128, "mb")


def test_floordiv_by_integer_unit_conversion_rounding_up():
    s = Size(8, "mb")
    result = s // 24
    assert result == Size(342, "kb")


def test_floordiv_by_integer_rounding_up():
    s = Size(10, "mb")
    result = s // 3
    assert result == Size(4, "mb")


def test_floordiv_by_integer_one_returns_same():
    s = Size(7, "gb")
    result = s // 1
    assert result == Size(7, "gb")


def test_floordiv_by_integer_zero_raises():
    s = Size(10, "mb")
    with pytest.raises(ZeroDivisionError):
        _ = s // 0
