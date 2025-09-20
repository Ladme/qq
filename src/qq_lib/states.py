# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime, timedelta
from enum import Enum
from typing import Self

from qq_lib.logger import get_logger

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

    def __str__(self):
        return self.name.lower()

    def __repr__(self):
        return str(self)

    @classmethod
    def fromStr(cls, s: str) -> Self:
        """
        Convert a string to the corresponding NaiveState enum variant.

        Args:
            s: String representation of the state (case-insensitive).

        Returns:
            NaiveState enum variant.
        """
        try:
            return cls[s.upper()]
        except KeyError:
            return cls.UNKNOWN


class BatchState(Enum):
    """
    State of the job according to the underlying batch system.
    """

    EXITING = 1
    HELD = 2
    QUEUED = 3
    RUNNING = 4
    MOVING = 5
    WAITING = 6
    SUSPENDED = 7
    FINISHED = 8
    UNKNOWN = 9

    def __str__(self):
        return self.name.lower()

    def __repr__(self):
        return str(self)

    @classmethod
    def _codeToState(cls) -> dict[str, str]:
        return {
            "E": "exiting",
            "H": "held",
            "Q": "queued",
            "R": "running",
            "T": "moving",
            "W": "waiting",
            "S": "suspended",
            "F": "finished",
        }

    @classmethod
    def fromCode(cls, code: str) -> Self:
        """Convert one-letter code to enum variant."""
        code = code.upper()
        if code not in cls._codeToState():
            return cls.UNKNOWN

        name = cls._codeToState()[code].upper()
        return cls[name]

    def toCode(self) -> str:
        """Return the one-letter code for this enum variant."""
        for k, v in self._codeToState().items():
            if v.upper() == self.name:
                return k

        return "?"


class QQState(Enum):
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
    IN_AN_INCONSISTENT_STATE = 10
    UNKNOWN = 11

    def __str__(self):
        return self.name.lower().replace("_", " ")

    def __repr__(self):
        return str(self)

    @classmethod
    def fromStates(cls, naive_state: NaiveState, batch_state: BatchState) -> Self:
        logger.debug(f"Converting to QQState from '{naive_state}' and '{batch_state}'.")
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

            case (NaiveState.KILLED, _):
                return cls.KILLED

            case (NaiveState.FINISHED, _):
                return cls.FINISHED

            case (NaiveState.FAILED, _):
                return cls.FAILED

        return cls.UNKNOWN

    def info(
        self,
        start_time: datetime,
        end_time: datetime,
        return_code: int | None,
        node: str | None,
    ) -> tuple[str, str]:
        match self:
            case QQState.QUEUED:
                return (
                    "Job is queued",
                    f"In queue for {format_duration(end_time - start_time)}",
                )
            case QQState.HELD:
                return (
                    "Job is held",
                    f"In queue for {format_duration(end_time - start_time)}",
                )
            case QQState.SUSPENDED:
                return ("Job is suspended", "")
            case QQState.WAITING:
                return (
                    "Job is waiting",
                    f"In queue for {format_duration(end_time - start_time)}",
                )
            case QQState.RUNNING:
                return (
                    "Job is running",
                    f"Running for {format_duration(end_time - start_time)} on '{node}'",
                )
            case QQState.BOOTING:
                return ("Job is booting", "Preparing the working directory...")
            case QQState.KILLED:
                return ("Job has been killed", f"Killed at {end_time}")
            case QQState.FAILED:
                return (
                    "Job has failed",
                    f"Failed at {end_time} [exit code: {return_code}]",
                )
            case QQState.FINISHED:
                return ("Job has finished", f"Completed at {end_time}")
            case QQState.IN_AN_INCONSISTENT_STATE:
                return (
                    "Job is in an inconsistent state",
                    "The batch system and qq disagree on the status of the job",
                )
            case QQState.UNKNOWN:
                return (
                    "Job is in an unknown state",
                    "Job is in a state that qq does not recognize",
                )

        return (
            "Job is in an unknown state",
            "Job is in a state that qq does not recognize",
        )

    @property
    def color(self) -> str:
        return {
            self.QUEUED: "magenta",
            self.HELD: "magenta",
            self.SUSPENDED: "yellow",
            self.WAITING: "magenta",
            self.RUNNING: "blue",
            self.BOOTING: "cyan",
            self.KILLED: "red",
            self.FAILED: "red",
            self.FINISHED: "green",
            self.IN_AN_INCONSISTENT_STATE: "grey70",
            self.UNKNOWN: "grey70",
        }[self]


def format_duration(td: timedelta) -> str:
    """
    Format a timedelta intelligently, showing only relevant units.
    """
    total_seconds = int(td.total_seconds())
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0 or days > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0 or days > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")

    return " ".join(parts)
