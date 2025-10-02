# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
from datetime import timedelta
from pathlib import Path

import readchar
from rich.live import Live
from rich.text import Text

from qq_lib.constants import QQ_INFO_SUFFIX
from qq_lib.error import QQError
from qq_lib.logger import get_logger

logger = get_logger(__name__)


def get_files_with_suffix(directory: Path, suffix: str) -> list[Path]:
    """
    Retrieve all files in a directory that have the specified file suffix.

    Args:
        directory (Path): The directory to search in.
        suffix (str): The file suffix to match (including the dot, e.g., '.txt').

    Returns:
        list[Path]: A list of Path objects representing files with the given suffix.
    """
    files = []
    for file in directory.iterdir():
        if file.is_file() and file.suffix == suffix:
            files.append(file)

    return files


def get_info_file(directory: Path) -> Path:
    """
    Locate the qq job info file in a directory.

    This function searches for files matching the `QQ_INFO_SUFFIX` in the
    provided directory. It raises an error if none or multiple info files are found.

    Args:
        directory (Path): The directory to search in.

    Returns:
        Path: The Path object of the detected qq job info file.

    Raises:
        QQError: If no info file is found or multiple info files are detected.
    """
    info_files = get_info_files(directory)
    if len(info_files) == 0:
        raise QQError("No qq job info file found.")
    if len(info_files) > 1:
        raise QQError("Multiple qq job info files found.")

    return info_files[0]


def get_info_files(directory: Path) -> list[Path]:
    """
    Retrieve all qq job info files in a directory.

    This function searches for files matching the `QQ_INFO_SUFFIX` in the
    provided directory. The files are sorted by their last modification time
    (with the newest modified file being last in the list).

    Args:
        directory (Path): The directory to search in.

    Returns:
        list[Path]: A list of Path objects representing the detected qq job info files.
    """
    info_files = get_files_with_suffix(directory, QQ_INFO_SUFFIX)
    logger.debug(f"Detected the following qq info files: {info_files}.")

    return sorted(info_files, key=lambda f: f.stat().st_mtime)


def yes_or_no_prompt(prompt: str) -> bool:
    """
    Display an interactive yes/no prompt to the user and return the selection.

    The prompt highlights the pressed key ('y' in green for yes, 'N' in red for no)
    and defaults to 'No' if the user presses any key other than 'y'.

    Args:
        prompt (str): The text to display as the question.

    Returns:
        bool: True if the user selects 'yes' (presses 'y'), False otherwise.
    """
    prompt = f"   {prompt} "
    text = (
        Text("PROMPT", style="magenta")
        + Text(prompt, style="default")
        + Text("[y/N]", style="bold default")
    )

    with Live(text, refresh_per_second=1) as live:
        key = readchar.readkey().lower()

        # highlight the pressed key
        if key == "y":
            choice = (
                Text("[", style="bold default")
                + Text("y", style="bold green")
                + Text("/N]", style="bold default")
            )
        else:
            choice = (
                Text("[y/", style="bold default")
                + Text("N", style="bold red")
                + Text("]", style="bold default")
            )

        live.update(
            Text("PROMPT", style="magenta") + Text(prompt, style="default") + choice
        )

    return key == "y"


def format_duration(td: timedelta) -> str:
    """
    Convert a timedelta into a human-readable string showing only relevant units.

    The output string includes days, hours, minutes, and seconds, but omits
    units that are zero unless a larger unit is present.

    Args:
        td (timedelta): The duration to format.

    Returns:
        str: A formatted string representing the duration, e.g., '1d 2h 3m 4s'.
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


def equals_normalized(a: str, b: str) -> bool:
    """
    Compare two strings for equality, ignoring case, hyphens, and underscores.

    Args:
        a (str): First string to compare.
        b (str): Second string to compare.

    Returns:
        bool: True if the normalized strings are equal, False otherwise.
    """

    def normalize(s: str) -> str:
        return s.lower().replace("-", "").replace("_", "")

    return normalize(a) == normalize(b)


def convert_absolute_to_relative(files: list[Path], target: Path) -> list[Path]:
    """
    Convert a list of absolute paths into paths relative to a target directory.

    Each file in `files` must be located inside `target` or one of its
    subdirectories. If any file is outside `target`, a `QQError` is raised.

    This function works even for remote files or paths to non-existent files.

    Args:
        files (list[Path]): A list of absolute file paths to convert.
        target (Path): The target directory against which paths are made relative.

    Returns:
        list[Path]: A list of paths relative to `target`.

    Raises:
        QQError: If any file in `files` is not located within `target`.
    """
    relative = []
    target_parts = target.parts

    for file in files:
        file_parts = file.parts

        # file must starts with the target path
        if file_parts[: len(target_parts)] != target_parts:
            raise QQError(f"Item '{file}' is not in target directory '{target}'.")

        # create a relative path
        rel_path = Path(*file_parts[len(target_parts) :])
        relative.append(rel_path)

    logger.debug(f"Converted paths: {relative}.")
    return relative


def wdhms_to_hhmmss(timestr: str) -> str:
    """
    Convert a time specification in the wdhms format into (H)HH:MM:SS.

    The accepted format is a sequence of one or more integer + unit tokens,
    where unit is one of:
      w = weeks, d = days, h = hours, m = minutes, s = seconds

    Tokens may be compact (e.g. "1w2d3h") or space-separated
    (e.g. "1w 2d 3h"). The function is case-insensitive.

    Examples:
      "1w2d3h4m5s" -> "195:04:05"
      "90m"         -> "1:30:00"
      ""            -> "0:00:00"

    Args:
        timestr: Input duration string in wdhms format.

    Returns:
        Converted time as a string in (H)HH:MM:SS.

    Raises:
        QQError: If the string contains invalid characters or does not
                 conform to the token pattern (excluding empty/whitespace,
                 which is treated as zero).
    """
    # treat empty / whitespace-only as zero
    if timestr.strip() == "":
        return "0:00:00"

    # validation
    full_pattern = re.compile(r"^\s*(?:\d+\s*[wdhms]\s*)+$", re.IGNORECASE)
    if not full_pattern.fullmatch(timestr):
        raise QQError(f"Invalid time string: '{timestr}'")

    # extract tokens
    token_pattern = re.compile(r"(\d+)\s*([wdhms])", re.IGNORECASE)
    matches = token_pattern.findall(timestr)

    weeks = days = hours = minutes = seconds = 0

    for value_str, unit in matches:
        value = int(value_str)
        unit = unit.lower()
        if unit == "w":
            weeks += value
        elif unit == "d":
            days += value
        elif unit == "h":
            hours += value
        elif unit == "m":
            minutes += value
        elif unit == "s":
            seconds += value

    total_seconds = (
        weeks * 7 * 24 * 3600 + days * 24 * 3600 + hours * 3600 + minutes * 60 + seconds
    )

    h, remainder = divmod(total_seconds, 3600)
    m, s = divmod(remainder, 60)

    return f"{h}:{m:02}:{s:02}"


def printf_to_regex(pattern: str) -> str:
    """
    Convert a simple printf-style pattern to an equivalent regular expression pattern.

    Args:
        pattern (str): A printf-style pattern (e.g., "md%04d", "file%03d_part%02d").

    Returns:
        str: A string representing the equivalent regex pattern.
    """
    regex = re.escape(pattern)
    regex = re.sub(r"%0(\d+)d", r"\\d{\1}", regex)  # double backslash
    regex = re.sub(r"%d", r"\\d+", regex)
    return f"^{regex}$"


def is_printf_pattern(pattern: str) -> bool:
    """
    Detect whether a string pattern uses printf-style numeric placeholders.

    Args:
        pattern (str): The pattern string to check.

    Returns:
        bool: True if the pattern contains printf-style placeholders, False otherwise.
    """
    return bool(re.search(r"%0?\d*d", pattern))
