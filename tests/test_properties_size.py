# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import math

import pytest

from qq_lib.core.error import QQError
from qq_lib.properties.size import Size


def test_init_and_str_repr():
    s = Size(10, "mb")
    assert str(s) == "10mb"
    assert repr(s) == "Size(value=10, unit='mb', round_func='<built-in function ceil>')"


def test_invalid_unit_raises():
    with pytest.raises(QQError):
        Size(5, "pb")


@pytest.mark.parametrize(
    "text, expected",
    [
        ("10mb", Size(10, "mb")),
        ("10 mb", Size(10, "mb")),
        ("2048KB", Size(2, "mb")),
        ("5tb", Size(5, "tb")),
    ],
)
def test_from_string_valid(text, expected):
    assert Size.fromString(text) == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        ("10mb", Size(10, "mb", math.floor)),
        ("10 mb", Size(10, "mb", math.floor)),
        ("2048KB", Size(2, "mb", math.floor)),
        ("5tb", Size(5, "tb", math.floor)),
    ],
)
def test_from_string_valid_floor(text, expected):
    assert Size.fromString(text, round_func=math.floor) == expected


def test_from_string_invalid():
    with pytest.raises(QQError):
        Size.fromString("nonsense")


@pytest.mark.parametrize(
    "value, unit, expected_value, expected_unit",
    [
        (2048, "kb", 2, "mb"),
        (1025, "kb", 2, "mb"),
        (1536, "kb", 2, "mb"),
        (1, "mb", 1, "mb"),
        (1024, "mb", 1, "gb"),
        (1536, "mb", 2, "gb"),
        (1, "kb", 1, "kb"),
        (0, "kb", 1, "kb"),
        (2048, "mb", 2, "gb"),
        (1048576, "kb", 1, "gb"),
        (1048577, "kb", 2, "gb"),
        (1073741824, "kb", 1, "tb"),
        (1073741825, "kb", 2, "tb"),
    ],
)
def test_post_init_conversions(value, unit, expected_value, expected_unit):
    s = Size(value, unit)
    assert s.value == expected_value
    assert s.unit == expected_unit


@pytest.mark.parametrize(
    "value, unit, expected_value, expected_unit",
    [
        (2048, "kb", 2, "mb"),
        (1025, "kb", 1, "mb"),
        (2047, "kb", 1, "mb"),
        (1, "mb", 1, "mb"),
        (1024, "mb", 1, "gb"),
        (1536, "mb", 1, "gb"),
        (1, "kb", 1, "kb"),
        (0, "kb", 1, "kb"),
        (2048, "mb", 2, "gb"),
        (1048576, "kb", 1, "gb"),
        (2097151, "kb", 1, "gb"),
        (1073741824, "kb", 1, "tb"),
        (2147483647, "kb", 1, "tb"),
    ],
)
def test_post_init_conversions_floor(value, unit, expected_value, expected_unit):
    s = Size(value, unit, round_func=math.floor)
    assert s.value == expected_value
    assert s.unit == expected_unit


def test_to_kb():
    assert Size(1, "kb").toKB() == 1
    assert Size(1, "mb").toKB() == 1024
    assert Size(1, "gb").toKB() == 1024 * 1024
    assert Size(1, "tb").toKB() == 1024 * 1024 * 1024


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


@pytest.mark.parametrize(
    "a,b,expected",
    [
        (Size(10, "mb"), Size(2, "mb"), 5.0),
        (Size(10, "mb"), Size(3, "mb"), pytest.approx(3.3333, rel=1e-3)),
        (Size(1, "gb"), Size(1, "gb"), 1.0),
        (Size(1, "gb"), Size(512, "mb"), 2.0),
        (Size(1024, "mb"), Size(1, "gb"), 1.0),
        (Size(2, "gb"), Size(1, "mb"), 2048.0),
        (Size(100, "mb"), Size(1, "gb"), pytest.approx(0.0976, rel=1e-3)),
        (Size(1, "kb"), Size(1, "kb"), 1.0),
        (Size(1, "gb"), Size(1, "kb"), 1024 * 1024.0),
        (Size(1, "tb"), Size(1, "gb"), 1024),
    ],
)
def test_truediv_size_valid(a, b, expected):
    result = a / b
    assert isinstance(result, float)
    assert result == expected


@pytest.mark.parametrize(
    "a,other",
    [
        (Size(10, "mb"), 2),
        (Size(10, "mb"), "2mb"),
        (Size(1, "gb"), None),
        (Size(1, "gb"), 3.14),
    ],
)
def test_truediv_type_error(a, other):
    with pytest.raises(TypeError):
        _ = a / other


@pytest.mark.parametrize(
    "a,b,expected",
    [
        (Size(1024, "KB"), Size(1, "mb"), 1.0),
        (Size(1, "GB"), Size(512, "MB"), 2.0),
    ],
)
def test_truediv_case_insensitive_units(a, b, expected):
    assert a / b == expected


@pytest.mark.parametrize(
    "kb,unit,expected_value",
    [
        (1024, "mb", 1),  # exact 1 MB
        (1025, "mb", 2),  # rounding up
        (1048576, "gb", 1),  # exact 1 GB
        (1048577, "gb", 2),  # rounding up
        (1536, "mb", 2),  # 1.5 MB - 2 MB (ceil)
        (1073741826, "tb", 2),  # rounding up tb
    ],
)
def test_size_from_kb_valid_conversions(kb, unit, expected_value):
    size = Size._fromKB(kb, unit)
    assert isinstance(size, Size)
    assert size.value == expected_value
    assert size.unit == unit


@pytest.mark.parametrize(
    "kb,unit,expected_value",
    [
        (1024, "mb", 1),  # exact 1 MB
        (1025, "mb", 1),  # rounding down
        (1048576, "gb", 1),  # exact 1 GB
        (2097151, "gb", 1),
        (1536, "mb", 1),  # 1.5 MB - 2 MB (floor)
        (1073741826, "tb", 1),  # rounding down tb
    ],
)
def test_size_from_kb_valid_conversions_floor(kb, unit, expected_value):
    size = Size._fromKB(kb, unit, round_func=math.floor)
    assert isinstance(size, Size)
    assert size.value == expected_value
    assert size.unit == unit


@pytest.mark.parametrize("invalid_unit", ["pb", "b", "", None])
def test_size_from_kb_invalid_unit(invalid_unit):
    with pytest.raises((KeyError, TypeError)):
        Size._fromKB(1024, invalid_unit)
