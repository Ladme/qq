# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
from dataclasses import fields
from pathlib import Path

from click import Parameter
from click_option_group import GroupedOption

from qq_lib.batch.interface import QQBatchInterface, QQBatchMeta
from qq_lib.core.common import split_files_list, to_snake_case
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.depend import Depend
from qq_lib.properties.job_type import QQJobType
from qq_lib.properties.resources import QQResources

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
        Parse the qq options from the script.

        Reads the script line by line, skipping the first line (shebang),
        and stops parsing at the first line that does not start with '# qq' or '#qq'
        (case-insensitive). Each valid line is split into key-value pairs, normalized
        to snake_case, and stored in `self._options`.

        Raises:
            QQError: If the script cannot be read, an option line is malformed or contains an unknown option.
        """
        if not self._script.is_file():
            raise QQError(f"Could not open '{self._script}' as a file.")

        with self._script.open() as f:
            # skip the first line (shebang)
            next(f, None)

            for line in f:
                stripped = line.strip()
                split = stripped.split()
                if stripped == "" or (
                    split[0].lower() != "#qq"
                    and (split[0] != "#" or split[1].lower() != "qq")
                ):
                    break  # stop parsing on the first non-qq line

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

    def getBatchSystem(self) -> type[QQBatchInterface] | None:
        """
        Return the batch system class specified in the script.

        Returns:
            type[QQBatchInterface] | None: The batch system class if specified, otherwise None.
        """
        if batch_system := self._options.get("batch_system"):
            return QQBatchMeta.fromStr(batch_system)

        return None

    def getQueue(self) -> str | None:
        """
        Return the queue specified for the job.

        Returns:
            str | None: Queue name, or None if not set.
        """
        return self._options.get("queue")

    def getJobType(self) -> QQJobType | None:
        """
        Return the job type specified in the script.

        Returns:
            QQJobType | None: Enum value representing the job type, or None if not set.
        """
        if job_type := self._options.get("job_type"):
            return QQJobType.fromStr(job_type)

        return None

    def getResources(self) -> QQResources:
        """
        Return the job resource specifications parsed from the script.

        Returns:
            QQResources: Resource requirements for the job.
        """
        field_names = {f.name for f in fields(QQResources)}
        # only select fields that are part of QQResources
        return QQResources(
            **{k: v for k, v in self._options.items() if k in field_names}
        )

    def getExclude(self) -> list[Path]:
        """
        Return a list of files to be excluded from the job submission.

        Returns:
            list[Path]: List of excluded file paths. Returns an empty list if none specified.
        """
        if exclude := self._options.get("exclude"):
            return split_files_list(exclude)

        return []

    def getNonInteractive(self) -> bool:
        """
        Return whether the job should be submitted in non-interactive mode.

        Returns:
            bool: True if non-interactive, False otherwise.
        """
        return self._options.get("non_interactive") == "true"

    def getLoopStart(self) -> int | None:
        """
        Return the starting cycle number for loop jobs.

        Returns:
            int | None: Start cycle, or None if not specified.
        """
        return self._options.get("loop_start")

    def getLoopEnd(self) -> int | None:
        """
        Return the ending cycle number for loop jobs.

        Returns:
            int | None: End cycle, or None if not specified.
        """
        return self._options.get("loop_end")

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
        return self._options.get("archive_format")

    def getDepend(self) -> list[Depend]:
        """
        Return the list of job dependencies.

        Returns:
            list[Depend]: List of job dependencies.
        """
        if raw := self._options.get("depend"):
            return Depend.multiFromStr(raw)

        return []

    @staticmethod
    def _stripAndSplit(string: str) -> list[str]:
        """
        Remove the leading `# qq` directive from a line and split the remaining content.

        Args:
            string (str): Input line to process.

        Returns:
            list[str]: A list with one or two elements depending on whether a split occurred.
        """
        content = re.sub(r"^#\s*qq\s*", "", string.strip(), flags=re.IGNORECASE)
        return re.split(r"[=\s]+", content, maxsplit=1)
