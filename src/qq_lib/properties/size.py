# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import math
import re
from dataclasses import dataclass
from typing import Self

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError


@dataclass(init=False)
class Size:
    """
    Represents a memory or storage size.

    The value is stored internally in kilobytes (kB). When converted to a string,
    it is displayed in the largest human-readable unit such that the relative
    rounding error does not exceed `CFG.size.max_rounding_error`.
    """

    value: int

    _unit_map = {
        "kb": 1,
        "mb": 1024,
        "gb": 1024 * 1024,
        "tb": 1024 * 1024 * 1024,
        "pb": 1024 * 1024 * 1024 * 1024,
    }

    def __init__(self, value: int, unit: str = "kb"):
        unit = unit.lower()
        if unit not in self._unit_map:
            # special handling of bytes
            if unit == "b":
                self.value = 0 if value == 0 else 1
                return

            raise QQError(f"Unsupported unit for size '{unit}'.")

        self.value = value * self._unit_map[unit]

    @classmethod
    def fromString(cls, s: str) -> Self:
        """
        Create a Size object from a string.

        Args:
            s (str): A string representation of the size, e.g., "10mb", "10 mb", "10m", "10M".

        Returns:
            Size: A Size instance with parsed value and unit.

        Raises:
            QQError: If the string cannot be parsed or contains an invalid unit.
        """
        match = re.match(r"^\s*(\d+)\s*([a-zA-Z]+)\s*$", s)
        if not match:
            raise QQError(f"Invalid size string: '{s}'.")
        value, unit = match.groups()

        # normalize single-letter units to their full form by appending 'b'
        # but skip bytes
        if len(unit) == 1 and unit != "b":
            unit = unit.lower() + "b"

        return cls(int(value), unit)

    def __mul__(self, n: int) -> "Size":
        """
        Multiply the Size by an integer.

        Args:
            n (int): The multiplier.

        Returns:
            Size: A new Size object with the scaled value.

        Raises:
            TypeError: If the multiplier is not an integer.
        """
        if not isinstance(n, int):
            return NotImplemented
        if n == 0:
            return Size(0, "kb")

        return Size(self.value * n, "kb")

    # allow 3 * Size
    __rmul__ = __mul__

    def __str__(self) -> str:
        for unit, factor in reversed(list(self._unit_map.items())):
            value = self.value / factor

            if value >= 1:
                rounded = round(value)
                # compute relative error from rounding
                approx_kb = rounded * factor
                error = abs(approx_kb - self.value) / self.value
                if error <= CFG.size.max_rounding_error or unit == "kb":
                    return f"{rounded}{unit}"
                # otherwise, try smaller unit

        # should not get here
        return f"{self.value}kb"

    def toStrExact(self) -> str:
        """Convert the Size to string while keeping it in kilobytes."""
        return f"{self.value}kb"

    def toStrExactSlurm(self) -> str:
        """Convert the Size to string while keeping it in kilobytes. Use K for the unit."""
        return f"{self.value}K"

    def __floordiv__(self, n: int) -> "Size":
        """
        Divide the Size by an integer.

        Args:
            n (int): The divisor.

        Returns:
            Size: A new Size object representing the divided size.

        Raises:
            TypeError: If n is not an integer.
            ZeroDivisionError: If n is zero.
        """
        if not isinstance(n, int):
            return NotImplemented
        if n == 0:
            raise ZeroDivisionError("Division by zero.")

        return Size(math.ceil(self.value / n), "kb")

    def __truediv__(self, other: "Size") -> float:
        """
        Perform true division (/) between two Size instances.

        Computes the ratio of this Size to another, expressed as a float.

        Args:
            other (Size): The divisor Size instance.

        Returns:
            float: The ratio of self to other, based on total kilobytes.

        Raises:
            TypeError:
                If `other` is not a Size instance.
            ZeroDivisionError:
                If `other` is a zero Size.
        """
        if not isinstance(other, Size):
            raise TypeError(
                f"Unsupported operand type(s) for /: 'Size' and '{type(other).__name__}'"
            )

        if other.value == 0:
            raise ZeroDivisionError("Division by zero size.")

        return self.value / other.value

    def __sub__(self, other: "Size") -> "Size":
        """
        Subtract one Size from another.

        Args:
            other (Size): The Size instance to subtract.

        Returns:
            Size: A new Size instance representing the difference.

        Raises:
            TypeError: If `other` is not a Size instance.
            ValueError: If the result would be negative.
        """
        if not isinstance(other, Size):
            raise TypeError(
                f"Unsupported operand type(s) for -: 'Size' and '{type(other).__name__}'"
            )

        result_kb = self.value - other.value
        if result_kb < 0:
            raise ValueError("Resulting Size cannot be negative.")

        return Size(result_kb, "kb")
