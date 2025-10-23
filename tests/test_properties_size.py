# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import pytest

from qq_lib.core.error import QQError
from qq_lib.properties.size import Size


def test_invalid_unit_raises():
    with pytest.raises(QQError):
        Size(5, "xb")


@pytest.mark.parametrize(
    "text, expected",
    [
        ("10mb", Size(10, "mb")),
        ("10 mb", Size(10, "mb")),
        ("2048KB", Size(2, "mb")),
        ("5tb", Size(5, "tb")),
        ("1pb", Size(1, "pb")),
        ("0b", Size(0, "kb")),
        ("3b", Size(1, "kb")),
    ],
)
def test_from_string_valid(text, expected):
    assert Size.fromString(text) == expected


def test_from_string_invalid():
    with pytest.raises(QQError):
        Size.fromString("nonsense")


def test_init_without_unit():
    assert Size(512) == Size(512, "kb")


@pytest.mark.parametrize(
    "value, unit, expected_value",
    [
        (2048, "kb", 2048),
        (1025, "kb", 1025),
        (1536, "kb", 1536),
        (1, "mb", 1024),
        (1024, "mb", 1048576),
        (1536, "mb", 1572864),
        (1, "kb", 1),
        (0, "kb", 0),
        (6, "gb", 6291456),
        (2, "tb", 2147483648),
        (1048576, "kb", 1048576),
        (1048577, "kb", 1048577),
        (5, "pb", 5497558138880),
        (0, "b", 0),
        (5, "b", 1),
    ],
)
def test_init_conversions(value, unit, expected_value):
    s = Size(value, unit)
    assert s.value == expected_value


@pytest.mark.parametrize(
    "value, expected_string",
    [
        (0, "0kb"),
        (128, "128kb"),
        (1024, "1mb"),
        (1025, "1mb"),
        (1126, "1mb"),
        (1139, "1139kb"),
        (337920, "330mb"),
        (338420, "330mb"),
        (337420, "330mb"),
        (338470, "331mb"),
        (1048576, "1gb"),
        (1050000, "1gb"),
        (1200000, "1172mb"),
        (1990000, "2gb"),
        (1073741824, "1tb"),
        (1573741824, "1501gb"),
        (5491558138880, "5pb"),
    ],
)
def test_str_conversions(value, expected_string):
    s = Size(value, "kb")
    assert str(s) == expected_string


@pytest.mark.parametrize(
    "value, expected_string",
    [
        (0, "0kb"),
        (128, "128kb"),
        (1024, "1024kb"),
        (1025, "1025kb"),
        (1126, "1126kb"),
        (1139, "1139kb"),
        (337920, "337920kb"),
        (338420, "338420kb"),
        (1048576, "1048576kb"),
        (1990000, "1990000kb"),
        (1073741824, "1073741824kb"),
        (1573741824, "1573741824kb"),
    ],
)
def test_to_str_exact(value, expected_string):
    s = Size(value, "kb")
    assert s.toStrExact() == expected_string


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
    assert result == Size(3414, "kb")


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


def test_truediv_division_by_zero_error():
    with pytest.raises(ZeroDivisionError):
        _ = Size(10, "mb") / Size(0, "kb")


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
    "a,b,expected_value",
    [
        (Size(200, "kb"), Size(100, "kb"), 100),
        (Size(10, "mb"), Size(5, "mb"), 5120),
        (Size(3, "gb"), Size(2, "gb"), 1048576),
        (Size(1, "gb"), Size(512, "mb"), 524288),
        (Size(1, "tb"), Size(512, "gb"), 536870912),
        (Size(512, "mb"), Size(512, "mb"), 0),
        (Size(2048, "mb"), Size(1, "gb"), 1048576),
        (Size(1025, "kb"), Size(1, "kb"), 1024),
    ],
)
def test_subtraction_valid(a, b, expected_value):
    result = a - b
    assert isinstance(result, Size)
    assert result.value == expected_value


def test_subtraction_negative_result_raises():
    a = Size(1, "gb")
    b = Size(2, "gb")
    with pytest.raises(ValueError, match="Resulting Size cannot be negative"):
        _ = a - b


def test_subtraction_invalid_type():
    a = Size(1, "gb")
    with pytest.raises(TypeError):
        _ = a - 100  # ty: ignore[unsupported-operator]
