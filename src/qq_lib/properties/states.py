# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from enum import Enum
from typing import Self

from qq_lib.core.logger import get_logger

logger = get_logger(__name__)


class NaiveState(Enum):
    """
    Naive state of the job written into qqinfo files.
    """

    QUEUED = 1
    RUNNING = 2
    FAILED = 3
    FINISHED = 4
    KILLED = 5
    UNKNOWN = 6

    def __str__(self) -> str:
        """
        Return the lowercase string representation of the enum variant.

        Returns:
            str: The name of the state in lowercase.
        """
        return self.name.lower()

    @classmethod
    def fromStr(cls, s: str) -> Self:
        """
        Convert a string to the corresponding NaiveState enum variant.

        Args:
            s (str): String representation of the state (case-insensitive).

        Returns:
            NaiveState: Corresponding enum variant. Returns UNKNOWN if no match is found.
        """
        try:
            return cls[s.upper()]
        except KeyError:
            return cls.UNKNOWN


class BatchState(Enum):
    """
    State of the job according to the underlying batch system.
    """

    RUNNING = 1
    QUEUED = 2
    FINISHED = 3
    FAILED = 4
    HELD = 5
    EXITING = 6
    WAITING = 7
    MOVING = 8
    SUSPENDED = 9
    UNKNOWN = 10

    def __str__(self) -> str:
        """
        Return the lowercase string representation of the enum variant.

        Returns:
            str: The name of the batch state in lowercase.
        """
        return self.name.lower()

    @classmethod
    def _codeToState(cls) -> dict[str, str]:
        """
        Internal mapping from one-letter codes to batch state names.

        Returns:
            dict[str, str]: Mapping of codes to corresponding batch state names.
        """
        return {
            "E": "exiting",
            "H": "held",
            "Q": "queued",
            "R": "running",
            "T": "moving",
            "W": "waiting",
            "S": "suspended",
            "F": "finished",
            "X": "failed",
        }

    @classmethod
    def fromCode(cls, code: str) -> Self:
        """
        Convert a one-letter batch system code to a BatchState enum variant.

        Args:
            code (str): One-letter code representing the batch system state.

        Returns:
            BatchState: Corresponding enum variant, or UNKNOWN if the code is invalid.
        """
        code = code.upper()
        if code not in cls._codeToState():
            return cls.UNKNOWN

        name = cls._codeToState()[code].upper()
        return cls[name]

    def toCode(self) -> str:
        """
        Return the one-letter code corresponding to this BatchState.

        Returns:
            str: One-letter code representing the batch state. Returns '?' if unknown.
        """
        for k, v in self._codeToState().items():
            if v.upper() == self.name:
                return k

        return "?"

    @property
    def color(self) -> str:
        """
        Return the display color associated with this BatchState.

        Returns:
            str: A string representing the color for presentation purposes.
        """
        return {
            self.QUEUED: RealState.QUEUED.color,
            self.HELD: RealState.HELD.color,
            self.SUSPENDED: RealState.SUSPENDED.color,
            self.WAITING: RealState.WAITING.color,
            self.RUNNING: RealState.RUNNING.color,
            self.FAILED: RealState.FAILED.color,
            self.FINISHED: RealState.FINISHED.color,
            self.EXITING: RealState.EXITING.color,
            self.MOVING: RealState.QUEUED.color,
            self.UNKNOWN: RealState.UNKNOWN.color,
        }[self]


class RealState(Enum):
    """
    Precise state of the job obtained by combining information from NaiveState and BatchState.
    """

    QUEUED = 1
    HELD = 2
    SUSPENDED = 3
    WAITING = 4
    RUNNING = 5
    BOOTING = 6
    KILLED = 7
    FAILED = 8
    FINISHED = 9
    EXITING = 10
    IN_AN_INCONSISTENT_STATE = 11
    UNKNOWN = 12

    def __str__(self) -> str:
        """
        Return the human-readable string representation of the state.

        Returns:
            str: Lowercase string with underscores replaced by spaces.
        """
        return self.name.lower().replace("_", " ")

    @classmethod
    def fromStates(cls, naive_state: NaiveState, batch_state: BatchState) -> Self:
        """
        Determine the RealState of a job based on its NaiveState and BatchState.

        Args:
            naive_state (NaiveState): The naive state of the job from qqinfo.
            batch_state (BatchState): The state of the job according to the batch system.

        Returns:
            RealState: The corresponding RealState.
        """
        logger.debug(
            f"Converting to RealState from '{naive_state}' and '{batch_state}'."
        )
        match (naive_state, batch_state):
            case (NaiveState.UNKNOWN, _):
                return cls.UNKNOWN

            case (NaiveState.QUEUED, BatchState.QUEUED | BatchState.MOVING):
                return cls.QUEUED
            case (NaiveState.QUEUED, BatchState.HELD):
                return cls.HELD
            case (NaiveState.QUEUED, BatchState.SUSPENDED):
                return cls.SUSPENDED
            case (NaiveState.QUEUED, BatchState.WAITING):
                return cls.WAITING
            case (NaiveState.QUEUED, BatchState.RUNNING):
                return cls.BOOTING
            case (NaiveState.QUEUED, _):
                return cls.IN_AN_INCONSISTENT_STATE

            case (NaiveState.RUNNING, BatchState.RUNNING):
                return cls.RUNNING
            case (NaiveState.RUNNING, BatchState.SUSPENDED):
                return cls.SUSPENDED
            case (NaiveState.RUNNING, _):
                return cls.IN_AN_INCONSISTENT_STATE

            case (NaiveState.KILLED, BatchState.RUNNING):
                return cls.EXITING
            case (NaiveState.KILLED, _):
                return cls.KILLED

            case (NaiveState.FINISHED, BatchState.RUNNING):
                return cls.EXITING
            case (
                NaiveState.FINISHED,
                BatchState.QUEUED
                | BatchState.WAITING
                | BatchState.HELD
                | BatchState.FAILED,
            ):
                return cls.IN_AN_INCONSISTENT_STATE
            case (NaiveState.FINISHED, _):
                return cls.FINISHED

            case (NaiveState.FAILED, BatchState.RUNNING):
                return cls.EXITING
            case (
                NaiveState.FAILED,
                BatchState.QUEUED
                | BatchState.WAITING
                | BatchState.HELD
                | BatchState.FINISHED,
            ):
                return cls.IN_AN_INCONSISTENT_STATE
            case (NaiveState.FAILED, _):
                return cls.FAILED

        return cls.UNKNOWN

    @property
    def color(self) -> str:
        """
        Return the display color associated with this RealState.

        Returns:
            str: A string representing the color for presentation purposes.
        """
        return {
            self.QUEUED: "bright_magenta",
            self.HELD: "bright_magenta",
            self.SUSPENDED: "bright_black",
            self.WAITING: "bright_magenta",
            self.RUNNING: "bright_blue",
            self.BOOTING: "bright_cyan",
            self.KILLED: "bright_red",
            self.FAILED: "bright_red",
            self.FINISHED: "bright_green",
            self.EXITING: "bright_yellow",
            self.IN_AN_INCONSISTENT_STATE: "grey70",
            self.UNKNOWN: "grey70",
        }[self]
