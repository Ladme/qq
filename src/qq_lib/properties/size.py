# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Self

from qq_lib.core.error import QQError


@dataclass(init=False)
class Size:
    """
    Represents a memory or disk size with an associated unit (kb, mb, gb, tb).

    Units are normalized to lowercase and use binary multiples (powers of 1024).

    The numeric value is automatically converted to the largest unit
    where the value remains >= 1. The rounding behavior depends on the
    provided `round_func`:

    - By default, values are rounded **up**. This is appropriate when
      specifying resource requirements, since the actual allocation
      must be at least as large as the user's request.
    - Rounding **down** should be used when reporting available or allocated
      storage or memory on a node.
    """

    value: int
    unit: str  # "kb", "mb", "gb", "tb"

    _unit_map = {"kb": 1, "mb": 1024, "gb": 1024 * 1024, "tb": 1024 * 1024 * 1024}

    def __init__(self, value: int, unit: str, round_func: Callable = math.ceil):
        self.value = value
        self.unit = unit
        self._round_func = round_func

        self.__post_init__()

    def __post_init__(self):
        """
        Normalize the unit, validate it, and convert the value to the largest possible
        unit where it is >= 1. Defaults to 1 kb if smaller.

        Raises:
            QQError: If the unit is not supported.
        """
        self.unit = self.unit.lower()
        if self.unit not in self._unit_map:
            raise QQError(f"Unsupported unit for size: '{self.unit}'.")

        # convert total KB to the largest unit possible where value >= 1
        total_kb = self.toKB()
        for unit, factor in reversed(list(self._unit_map.items())):
            if total_kb >= factor:
                self.value = self._round_func(total_kb / factor)
                self.unit = unit
                break
        else:
            # if total_kb < 1 kb, fallback to 1 kb
            self.value = 1
            self.unit = "kb"

    def __getstate__(self):
        return {"value": self.value, "unit": self.unit}

    @classmethod
    def fromString(cls, s: str, round_func: Callable = math.ceil) -> Self:
        """
        Create a Size object from a string.

        Args:
            s (str): A string representation of the size, e.g., "10mb", "10 mb".

        Returns:
            Size: A Size instance with parsed value and unit.

        Raises:
            QQError: If the string cannot be parsed or contains an invalid unit.
        """
        match = re.match(r"^\s*(\d+)\s*([a-zA-Z]+)\s*$", s)
        if not match:
            raise QQError(f"Invalid size string: '{s}'.")
        value, unit = match.groups()
        return cls(int(value), unit.lower(), round_func)

    def toKB(self) -> int:
        """
        Convert the Size to kilobytes.

        Returns:
            int: The size expressed in kilobytes.
        """
        return self.value * self._unit_map[self.unit]

    @classmethod
    def _fromKB(cls, kb: int, unit: str, round_func: Callable = math.ceil) -> Self:
        """
        Create a Size object from a value in kilobytes.

        Args:
            kb (int): The size in kilobytes.
            unit (str): The target unit ("kb", "mb", "gb", "tb").
            round_func (Callable): The function to use for rounding.
                Defaults to math.ceil, if not specified.

        Returns:
            Size: A new Size instance converted into the given unit.
        """
        factor = cls._unit_map[unit]
        value = round_func(kb / factor)
        return cls(value, unit, round_func)

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
            return Size(0, self.unit)

        return Size(self.toKB() * n, "kb")

    # allow 3 * Size
    __rmul__ = __mul__

    def __str__(self) -> str:
        return f"{self.value}{self.unit}"

    def __repr__(self) -> str:
        return f"Size(value={self.value}, unit='{self.unit}', _round_func='{self._round_func}')"

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
            raise ZeroDivisionError("division by zero")

        return Size(self._round_func(self.toKB() / n), "kb")

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
                If `other` has a zero total size.
        """
        if not isinstance(other, Size):
            raise TypeError(
                f"Unsupported operand type(s) for /: 'Size' and '{type(other).__name__}'"
            )

        other_kb = other.toKB()
        # the smallest Size is 1 kB, so this should never happen, but we keep it to be safe
        if other_kb == 0:
            raise ZeroDivisionError("division by zero size")

        return self.toKB() / other_kb
