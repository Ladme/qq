# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
from dataclasses import asdict, dataclass
from pathlib import Path

from qq_lib.archive.archiver import QQArchiver
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)


@dataclass(init=False)
class QQLoopInfo:
    """
    Dataclass containing information about a qq loop job.
    """

    start: int
    end: int
    archive: Path
    archive_format: str
    current: int

    def __init__(
        self,
        start: int,
        end: int | None,
        archive: Path,
        archive_format: str,
        current: int | None = None,
        job_dir: Path | None = None,
    ):
        """
        Initialize loop job information with validation checks.

        Args:
            start (int): The starting cycle number.
            end (int | None): The ending cycle number. Must be provided and >= `start`.
            archive (Path): Path to the archive directory.
            job_dir (Path | None): The job submission directory. Used to validate archive.
                If `None`, no validation is performed.
            archive_format (str): File naming pattern used for archived files.
            current (int | None): The current cycle number. Defaults to `start`
                if not provided.

        Raises:
            QQError: If `end` is not provided, if `start > end`, if `current > end`,
                or if the archive path is invalid.
        """
        if not end:
            raise QQError("Attribute 'loop-end' is undefined.")

        self.archive = archive.resolve()
        if job_dir and self.archive == job_dir.resolve():
            raise QQError("Job directory cannot be used as the loop job's archive.")

        self.archive_format = archive_format

        self.start = start
        self.end = end
        self.current = current or self._getCycle()

        if self.start < 0:
            raise QQError(f"Attribute 'loop-start' ({self.start}) cannot be negative.")

        if self.start > self.end:
            raise QQError(
                f"Attribute 'loop-start' ({self.start}) cannot be higher than 'loop-end' ({self.end})."
            )

        if self.current > self.end:
            raise QQError(
                f"Current cycle number ({self.current}) cannot be higher than 'loop-end' ({self.end})."
            )

    def toDict(self) -> dict[str, object]:
        """Return all fields as a dict."""
        return {
            k: str(v) if isinstance(v, Path) else v for k, v in asdict(self).items()
        }

    def _getCycle(self) -> int:
        """
        Determine the current cycle number based on files in the archive directory.

        Returns:
            int: The detected maximum cycle number, or `self.start` if no valid cycle
                can be inferred.

        Notes:
            - Only the first sequence of digits found in the stem is considered.
        """

        # if the directory does not exist, use the starting cycle number
        if not self.archive.is_dir():
            logger.debug(
                f"Archive '{self.archive}' does not exist. Setting cycle number to start ({self.start})."
            )
            return self.start

        stem_pattern = QQArchiver._prepare_regex_pattern(self.archive_format)
        logger.debug(f"Stem pattern: {stem_pattern}.")

        # use start as default
        max_number = self.start
        for f in self.archive.iterdir():
            if not stem_pattern.fullmatch(f.stem):
                continue

            match = re.search(r"\d+", f.stem)
            if match:
                number = int(match.group(0))
                max_number = max(max_number, number)

        return max_number
