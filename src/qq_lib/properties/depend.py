# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Representation and handling of qq job dependencies.

This module defines `DependType`, an enumeration of supported dependency
conditions and the `Depend` dataclass, which stores both the dependency type
and referenced job IDs.
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import Self

from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)


class DependType(Enum):
    """
    Enumeration of supported job dependency types.
    """

    # Job should start after other job has started.
    AFTER_START = 0

    # Job should start after other job has finished successfully.
    AFTER_SUCCESS = 1

    # Job should start after other job has failed or been killed.
    AFTER_FAILURE = 2

    # Job should start after other job has finished, failed or been killed.
    AFTER_COMPLETION = 3

    @classmethod
    def fromStr(cls, string: str) -> Self:
        """
        Convert a dependency string keyword to a `DependType`.

        Supported dependency keywords:
        - "after" - AFTER_START
        - "afterok" - AFTER_SUCCESS
        - "afternotok" - AFTER_FAILURE
        - "afterany" - AFTER_COMPLETION

        Args:
            string (str): Dependency type keyword.

        Returns:
            DependType: Corresponding dependency type enum.

        Raises:
            QQError: If the given string does not correspond to any known dependency type.
        """
        match string:
            case "after":
                return cls.AFTER_START
            case "afterok":
                return cls.AFTER_SUCCESS
            case "afternotok":
                return cls.AFTER_FAILURE
            case "afterany":
                return cls.AFTER_COMPLETION

        raise QQError(f"Unknown dependency type '{string}'")

    def toStr(self) -> str:
        """
        Convert the dependency type to its corresponding string keyword.

        Returns:
            str: One of the recognized dependency keywords:
                - "after" for AFTER_START
                - "afterok" for AFTER_SUCCESS
                - "afternotok" for AFTER_FAILURE
                - "afterany" for AFTER_COMPLETION

        Raises:
            QQError: If the dependency type is unrecognized (should not occur).
        """
        match self:
            case DependType.AFTER_START:
                return "after"
            case DependType.AFTER_SUCCESS:
                return "afterok"
            case DependType.AFTER_FAILURE:
                return "afternotok"
            case DependType.AFTER_COMPLETION:
                return "afterany"

        raise QQError(
            f"Unknown dependency type '{self}'. This is a bug; please report it."
        )


@dataclass
class Depend:
    """
    Representation of a parsed job dependency.

    A dependency defines both the type of dependency (e.g., after start, after success)
    and a list of job IDs that the current job depends on.

    Attributes:
        type (DependType): The type of dependency, determined by the dependency keyword.
        jobs (list[str]): List of job IDs this dependency refers to.
    """

    type: DependType
    jobs: list[str]

    @classmethod
    def fromStr(cls, raw_depend: str):
        """
        Initialize a `Depend` object by parsing a raw dependency specification.

        The expected format is `<type>=<job_id>[:<job_id>...]`, where:
        - `<type>` is one of `"after"`, `"afterok"`, `"afternotok"`, or `"afterany"`.
        - `<job_id>` values are colon-separated job identifiers.

        Args:
            raw_depend (str): Raw dependency string to parse.

        Raises:
            QQError: If the dependency string is malformed or cannot be parsed.
        """
        logger.debug(f"Depend string to parse: '{raw_depend}'.")
        try:
            raw_type, raw_jobs = raw_depend.split("=")
            jobs = raw_jobs.split(":")
            if any(x.strip() == "" for x in jobs):
                raise QQError("Missing job id.")
            type = DependType.fromStr(raw_type)
            return cls(type, jobs)
        except Exception as e:
            raise QQError(
                f"Could not parse dependency specification '{raw_depend}': {e}."
            ) from e

    @classmethod
    def multiFromStr(cls, raw: str) -> list[Self]:
        """
        Parse a combined dependency string into a list of `Depend` objects.

        The input may contain multiple dependency expressions separated by commas,
        spaces, or both. Each expression will be parsed into a separate `Depend` instance.

        Args:
            raw (str): Raw dependency string possibly containing multiple expressions.

        Returns:
            list[Depend]: A list of parsed dependency objects.

        Raises:
            QQError: If any dependency expression within the string is malformed.
        """
        logger.debug(f"Full depend string to parse: '{raw}'.")
        return [Depend.fromStr(dep) for dep in re.split(r"[,\s]+", raw.strip()) if dep]

    def toStr(self) -> str:
        """
        Convert the full Depend object to a string representation.

        Format:
            <type>=<job_id1>:<job_id2>:...

        Returns:
            str: String representation combining the dependency type keyword and
                the colon-separated list of job IDs.
        """
        return f"{self.type.toStr()}={':'.join(self.jobs)}"
