# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
from dataclasses import fields
from pathlib import Path

from click import Parameter
from click_option_group import GroupedOption

from qq_lib.batch.interface import BatchInterface, BatchMeta
from qq_lib.core.common import split_files_list, to_snake_case
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.depend import Depend
from qq_lib.properties.job_type import JobType
from qq_lib.properties.resources import Resources

logger = get_logger(__name__)


class QQParser:
    """
    Parser for qq job submission options specified in a script.
    """

    def __init__(self, script: Path, params: list[Parameter]):
        """
        Initialize the parser.

        Args:
            script (Path): Path to the qq job script to parse.
            params (list[Parameter]): List of click Parameter objects defining
                valid options. Only `GroupedOption` names are considered.
        """
        self._script = script
        self._known_options = {p.name for p in params if isinstance(p, GroupedOption)}
        logger.debug(
            f"Known options for QQParser: {self._known_options} ({len(self._known_options)} options)."
        )

        self._options: dict[str, object] = {}

    def parse(self) -> None:
        """
        Extract and parse `qq` options from the script.

        The method processes the script line by line, skipping the first line (shebang).
        It continues reading until it encounters a line that is not a `qq` directive,
        is non-empty, and is not a comment. Empty or commented lines are ignored.

        Each valid `qq` line is parsed into key-value pairs, normalized to `snake_case`,
        and stored in `self._options`.

        Raises:
            QQError: If the script cannot be read, or if an option line is malformed or
                    contains an unknown option.
        """
        if not self._script.is_file():
            raise QQError(f"Could not open '{self._script}' as a file.")

        with self._script.open() as f:
            # skip the first line (shebang)
            next(f, None)

            for line in f:
                stripped = line.strip()
                if stripped == "":
                    logger.debug("QQParser: skipping empty line.")
                    continue  # skip empty lines

                # check whether this is a qq command
                if not re.match(r"#\s*qq", stripped, re.IGNORECASE):
                    if stripped.startswith("#"):
                        logger.debug(f"QQParser: skipping commented line '{line}'.")
                        continue  # skip commented lines
                    logger.debug(f"QQParser: ending parsing at line '{line}'.")
                    break  # stop parsing at other lines

                # remove the leading '# qq' and split by whitespace or '='
                parts = QQParser._stripAndSplit(line)
                if len(parts) < 2:
                    raise QQError(
                        f"Invalid qq submit option line in '{str(self._script)}': {line}."
                    )

                key, value = parts[-2], parts[-1]
                snake_case_key = to_snake_case(key)

                # handle workdir and worksize where two forms of the keyword are allowed
                snake_case_key = snake_case_key.replace("workdir", "work_dir").replace(
                    "worksize", "work_size"
                )

                # is this a known option?
                if snake_case_key in self._known_options:
                    try:
                        self._options[snake_case_key] = int(value)
                    except ValueError:
                        self._options[snake_case_key] = value
                else:
                    raise QQError(
                        f"Unknown qq submit option '{key}' in '{str(self._script)}': {line}.\nKnown options are '{' '.join(self._known_options)}'."
                    )

        logger.debug(f"Parsed options from '{self._script}': {self._options}.")

    def getBatchSystem(self) -> type[BatchInterface] | None:
        """
        Return the batch system class specified in the script.

        Returns:
            type[BatchInterface] | None: The batch system class if specified, otherwise None.
        """
        if isinstance(batch_system := self._options.get("batch_system"), str):
            return BatchMeta.fromStr(batch_system)

        return None

    def getQueue(self) -> str | None:
        """
        Return the queue specified for the job.

        Returns:
            str | None: Queue name, or None if not set.
        """
        if not isinstance(queue := self._options.get("queue"), str):
            return None
        return queue

    def getJobType(self) -> JobType | None:
        """
        Return the job type specified in the script.

        Returns:
            JobType | None: Enum value representing the job type, or None if not set.
        """
        if isinstance(job_type := self._options.get("job_type"), str):
            return JobType.fromStr(job_type)

        return None

    def getResources(self) -> Resources:
        """
        Return the job resource specifications parsed from the script.

        Returns:
            Resources: Resource requirements for the job.
        """
        field_names = {f.name for f in fields(Resources)}
        # only select fields that are part of Resources
        return Resources(**{k: v for k, v in self._options.items() if k in field_names})

    def getExclude(self) -> list[Path]:
        """
        Return a list of files to be excluded from the job submission.

        Returns:
            list[Path]: List of excluded file paths. Returns an empty list if none specified.
        """
        if isinstance(exclude := self._options.get("exclude"), str):
            return split_files_list(exclude)

        return []

    def getLoopStart(self) -> int | None:
        """
        Return the starting cycle number for loop jobs.

        Returns:
            int | None: Start cycle, or None if not specified.
        """
        if isinstance(loop_start := self._options.get("loop_start"), int):
            return loop_start
        return None

    def getLoopEnd(self) -> int | None:
        """
        Return the ending cycle number for loop jobs.

        Returns:
            int | None: End cycle, or None if not specified.
        """
        if isinstance(loop_end := self._options.get("loop_end"), int):
            return loop_end
        return None

    def getArchive(self) -> Path | None:
        """
        Return the archive directory path specified in the script.

        Returns:
            Path | None: Archive directory path, or None if not set.
        """
        if archive := self._options.get("archive"):
            return Path(archive)

        return None

    def getArchiveFormat(self) -> str | None:
        """
        Return the file naming format used for archived files.

        Returns:
            str | None: Archive filename format string, or None if not set.
        """
        if isinstance(archive_format := self._options.get("archive_format"), str):
            return archive_format
        return None

    def getDepend(self) -> list[Depend]:
        """
        Return the list of job dependencies.

        Returns:
            list[Depend]: List of job dependencies.
        """
        if isinstance(raw := self._options.get("depend"), str):
            return Depend.multiFromStr(raw)

        return []

    def getAccount(self) -> str | None:
        """
        Get the account name to use for the job.

        Returns:
            str | None: The account name or None if not defined.
        """
        if isinstance(account := self._options.get("account"), str):
            return account

        return None

    @staticmethod
    def _stripAndSplit(string: str) -> list[str]:
        """
        Remove the leading `# qq` directive from a line, extract content before the next `#`
        (if any), and split the remaining content.

        Args:
            string (str): Input line to process.

        Returns:
            list[str]: A list with one or two elements depending on whether a split occurred.
        """
        match = re.search(
            r"^#\s*qq\s*(.*?)\s*(?:#|$)", string.strip(), flags=re.IGNORECASE
        )
        content = match.group(1).strip() if match else string.strip()

        # split by whitespace or '='
        return re.split(r"[=\s]+", content, maxsplit=1)
