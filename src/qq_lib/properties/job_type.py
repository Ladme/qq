# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from enum import Enum
from typing import Self

from qq_lib.core.error import QQError


class JobType(Enum):
    """
    Type of the qq job.
    """

    STANDARD = 1
    LOOP = 2

    def __str__(self):
        return self.name.lower()

    @classmethod
    def fromStr(cls, s: str) -> Self:
        """
        Convert a string to the corresponding JobType enum variant.

        Args:
            s (str): String representation of the job type (case-insensitive).

        Returns:
            JobType variant.

        Raises:
            QQError if the string corresponds to no JobType.
        """
        try:
            return cls[s.upper()]
        except KeyError:
            raise QQError(f"Could not recognize a job type '{s}'.")
