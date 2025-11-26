# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
General utility functions for the qq library.

This module provides helpers for working with qq job files, time durations,
YAML I/O, string normalization, user prompts, path manipulation, and job-name construction.
"""

import re
from datetime import timedelta
from functools import lru_cache
from pathlib import Path

import readchar
import yaml
from rich.console import Console
from rich.live import Live
from rich.text import Text

from .config import CFG
from .error import QQError
from .logger import get_logger

logger = get_logger(__name__)


@lru_cache(maxsize=1)
def load_yaml_dumper() -> type[yaml.Dumper]:
    """Return the fastest available YAML dumper (CDumper if possible)."""
    try:
        from yaml import CDumper as Dumper  # type: ignore[attr-defined]

        logger.debug("Loaded YAML CDumper.")
    except ImportError:
        from yaml import Dumper

        logger.debug("Loaded default YAML dumper.")
    return Dumper


@lru_cache(maxsize=1)
def load_yaml_loader() -> type[yaml.SafeLoader]:
    """Return the fastest available safe YAML loader (CSafeLoader if possible)."""
    try:
        from yaml import (
            CSafeLoader as SafeLoader,  # ty: ignore[possibly-missing-import]
        )

        logger.debug("Loaded YAML CLoader.")
    except ImportError:
        from yaml import SafeLoader

        logger.debug("Loaded default YAML loader.")

    return SafeLoader


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


def get_runtime_files(directory: Path) -> list[Path]:
    """
    Retrieve all qq runtime files in a directory.

    Args:
        directory (Path): The directory to search in.

    Returns:
        list[Path]: A list of Path objects representing qq runtime files.
    """
    files = []
    for suffix in CFG.suffixes.all_suffixes:
        files.extend(get_files_with_suffix(directory, suffix))

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
    info_files = get_files_with_suffix(directory, CFG.suffixes.qq_info)
    logger.debug(f"Detected the following qq info files: {info_files}.")

    return sorted(info_files, key=lambda f: f.stat().st_mtime)


def get_info_file_from_job_id(job_id: str) -> Path:
    """
    Get path to the qq info file corresponding to a job with the given ID.
    The batch system to use is obtained from the environment variable or guessed.

    Args:
        job_id (str): The ID of the job for which to retrieve the info file.

    Returns:
        Path: Absolute path to the qq job info file.

    Raises:
        QQError: If the batch system could not be guessed,
        the job does not exist or is not a qq job.
    """

    from qq_lib.batch.interface import (
        BatchJobInterface,
        BatchMeta,
    )

    BatchSystem = BatchMeta.fromEnvVarOrGuess()
    job_info: BatchJobInterface = BatchSystem.getBatchJob(job_id)

    if job_info.isEmpty():
        raise QQError(f"Job '{job_id}' does not exist.")

    if not (path := job_info.getInfoFile()):
        raise QQError(f"Job '{job_id}' is not a valid qq job.")

    return path


def get_info_files_from_job_id_or_dir(job_id: str | None) -> list[Path]:
    """
    Retrieve qq job info files based on a job ID or from the current directory.

    Args:
        job_id (str | None): The ID of the qq job to retrieve the info file for.
            If None, the function searches for qq job info files in the current directory.

    Returns:
        list[Path]: A list containing the qq job info file(s). If a job ID is provided,
            the list contains a single Path. If not, it contains all detected info files
            in the current directory.

    Raises:
        QQError: If the info file corresponding to the given job ID does not exist
            or is not reachable, or if no qq job info file is found in the current
            directory when no job ID is provided.
    """
    if job_id:
        info_file = get_info_file_from_job_id(job_id)
        # check that the detected info file exists and we have permissions to read it
        try:
            missing = not info_file.is_file()
        except PermissionError:
            missing = True

        if missing:
            raise QQError(
                f"Info file for job '{job_id}' does not exist or is not reachable."
            )

        return [info_file]

    # get info files from the directory
    info_files = get_info_files(Path())
    if not info_files:
        raise QQError("No qq job info file found.")

    return info_files


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

    The output string includes weeks, days, hours, minutes, and seconds, but omits
    units that are zero.

    Args:
        td (timedelta): The duration to format.

    Returns:
        str: A formatted string representing the duration, e.g., '1d 2h 3m 4s'.
    """
    total_seconds = int(td.total_seconds())

    days_total, remainder = divmod(total_seconds, 86400)
    weeks, days = divmod(days_total, 7)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if weeks > 0:
        parts.append(f"{weeks}w")
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0 or total_seconds == 0:
        parts.append(f"{seconds}s")

    return " ".join(parts)


def format_duration_wdhhmmss(td: timedelta) -> str:
    """
    Format a timedelta into a human-readable string: Xw Yd HH:MM:SS.

    Weeks and days are included only if non-zero.
    Hours, minutes, and seconds are always displayed with zero-padding.

    Examples:
        0:00:45         -> "00:00:45"
        1 day, 2:03:04  -> "1d 02:03:04"
        10 days, 5:06:07 -> "1w 3d 05:06:07"

    Args:
        td (timedelta): The duration to format.

    Returns:
        str: Formatted string in "Xw Yd HH:MM:SS" format.
    """
    total_seconds = int(td.total_seconds())

    weeks, remainder = divmod(total_seconds, 7 * 24 * 3600)
    days, remainder = divmod(remainder, 24 * 3600)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if weeks > 0:
        parts.append(f"{weeks}w")
    if days > 0:
        parts.append(f"{days}d")

    parts.append(f"{hours:02}:{minutes:02}:{seconds:02}")

    return " ".join(parts)


def hhmmss_to_duration(timestr: str) -> timedelta:
    """
    Convert a time string in HH:MM:SS (or HHH:MM:SS) format to a timedelta object.

    Examples:
        "0:00:00"   -> 0 seconds
        "1:23:45"   -> 1 hour, 23 minutes, 45 seconds
        "100:00:00" -> 100 hours

    Args:
        timestr (str): Input string in HH:MM:SS format.

    Returns:
        timedelta: The corresponding duration.

    Raises:
        QQError: If the input string is not in a valid HH:MM:SS format or contains
                    invalid numeric values.
    """
    pattern = re.compile(r"^\s*(\d+):([0-5]?\d):([0-5]?\d)\s*$")
    match = pattern.fullmatch(timestr)
    if not match:
        raise QQError(f"Invalid HH:MM:SS time string '{timestr}'.")

    hours, minutes, seconds = map(int, match.groups())

    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def dhhmmss_to_duration(timestr: str) -> timedelta:
    """
    Convert a time string in optional D-HH:MM:SS (or HH:MM:SS / HHH:MM:SS) format to a timedelta object.

    Examples:
        "0:00:00"      -> 0 seconds
        "1:23:45"      -> 1 hour, 23 minutes, 45 seconds
        "100:00:00"    -> 100 hours
        "2-12:34:56"   -> 2 days, 12 hours, 34 minutes, 56 seconds

    Args:
        timestr (str): Input string in optional D-HH:MM:SS format.

    Returns:
        timedelta: The corresponding duration.

    Raises:
        QQError: If the input string is not in a valid format or contains invalid numeric values.
    """
    pattern = re.compile(r"^\s*(?:(\d+)-)?(\d+):([0-5]?\d):([0-5]?\d)\s*$")
    match = pattern.fullmatch(timestr)
    if not match:
        raise QQError(f"Invalid D-HH:MM:SS time string '{timestr}'.")

    days_str, hours_str, minutes_str, seconds_str = match.groups()
    days = int(days_str) if days_str else 0
    hours = int(hours_str)
    minutes = int(minutes_str)
    seconds = int(seconds_str)

    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def normalize(s: str) -> str:
    """
    Normalize a string for consistent comparison.

    The string is converted to lowercase and all hyphens and underscores are removed.

    Args:
        s (str): The input string to normalize.

    Returns:
        str: The normalized string.
    """
    return s.lower().replace("-", "").replace("_", "")


def equals_normalized(a: str, b: str) -> bool:
    """
    Compare two strings for equality, ignoring case, hyphens, and underscores.

    Args:
        a (str): First string to compare.
        b (str): Second string to compare.

    Returns:
        bool: True if the normalized strings are equal, False otherwise.
    """

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
        raise QQError(f"Invalid time string '{timestr}'.")

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


def hhmmss_to_wdhms(timestr: str) -> str:
    """
    Convert a time specification in (H)HH:MM:SS format into the compact wdhms format.

    The output format expresses the duration as a sequence of one or more integer + unit tokens:
      w = weeks, d = days, h = hours, m = minutes, s = seconds

    Units that are zero are omitted, except that "0s" is returned if the total duration is zero.

    Examples:
        "195:04:05" -> "1w2d3h4m5s"
        "1:30:00"   -> "1h30m"
        "0:00:00"   -> "0s"
        "49:00:00"  -> "2d1h"

    Args:
        timestr (str): Input time string in (H)HH:MM:SS format.

    Returns:
        str: Time duration converted into the compact wdhms format.

    Raises:
        QQError: If the input string is malformed or does not conform
                 to the expected (H)HH:MM:SS pattern.
    """
    pattern = re.compile(r"^\s*(\d+):([0-5]?\d):([0-5]?\d)\s*$")
    match = pattern.fullmatch(timestr)
    if not match:
        raise QQError(f"Invalid HH:MM:SS time string '{timestr}'.")

    hours, minutes, seconds = map(int, match.groups())
    total_seconds = hours * 3600 + minutes * 60 + seconds

    if total_seconds == 0:
        return "0s"

    weeks, remainder = divmod(total_seconds, 7 * 24 * 3600)
    days, remainder = divmod(remainder, 24 * 3600)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if weeks:
        parts.append(f"{weeks}w")
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds:
        parts.append(f"{seconds}s")

    return "".join(parts)


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
    return re.sub(r"%d", r"\\d+", regex)


def is_printf_pattern(pattern: str) -> bool:
    """
    Detect whether a string pattern uses printf-style numeric placeholders.

    Args:
        pattern (str): The pattern string to check.

    Returns:
        bool: True if the pattern contains printf-style placeholders, False otherwise.
    """
    return bool(re.search(r"%0?\d*d", pattern))


def split_files_list(string: str | None) -> list[Path]:
    """
    Split a string containing multiple file paths into a list of relative Path objects.

    The string can contain file paths separated by colons (:), commas (,), or
    any whitespace characters (space, tab, newline).

    Args:
        string (str | None): The string containing file paths. If None or empty,
                             an empty list is returned.

    Returns:
        list[Path]: A list of Path objects corresponding to the individual
                    relative file paths in the input string.
    """
    if not string:
        return []

    return [Path(f) for f in re.split(r"[:,\s]+", string)]


def to_snake_case(s: str) -> str:
    """
    Convert a string from PascalCase or kebab-case to snake_case.

    Args:
        s (str): Input string in PascalCase or kebab-case.

    Returns:
        str: Converted string in snake_case.
    """
    # replace hyphens with underscores
    s = s.replace("-", "_")

    # convert PascalCase to snake_case
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


def get_panel_width(
    console: Console, factor: int, min_width: int | None, max_width: int | None
):
    """
    Calculate the width of a panel relative to the console width, constrained by
    optional minimum and maximum width values.

    Args:
        console (Console): A rich Console-like object that provides terminal size.
        factor (int): A divisor used to scale down the terminal width.
        min_width (int): The minimum allowable panel width. If None, no lower bound is applied.
        max_width (int): The maximum allowable panel width. If None, no upper bound is applied.

    Returns:
        int: The computed panel width after applying scaling and bounds.
    """

    term_width = console.size.width
    panel_width = term_width // factor
    if min_width is not None:
        panel_width = max(panel_width, min_width)
    if max_width is not None:
        panel_width = min(panel_width, max_width)

    return panel_width


def construct_loop_job_name(script_name: str, cycle: int) -> str:
    """
    Construct a job name for a loop job.

    Args:
        script_name (str): Filename of the submitted script.
        cycle (int): The current cycle of the loop job.

    Returns:
        str: The name of the loop job in the current cycle.
    """
    try:
        # if the script has an extension, put the cycle number BEFORE the extension
        stem, suffix = script_name.split(".", maxsplit=1)
        return f"{stem}{CFG.loop_jobs.pattern % cycle}.{suffix}"
    except ValueError:
        # if the script has no extension, add the cycle number after the full name
        return f"{script_name}{CFG.loop_jobs.pattern % cycle}"


def construct_info_file_path(input_dir: Path, job_name: str) -> Path:
    """
    Construct the absolute path to a job's qq info file.

    Args:
        input_dir (Path): The directory containing the job script.
        job_name (str): The name of the job.

    Returns:
        Path: The absolute path to the job's qq info file.
    """
    return (input_dir / job_name).with_suffix(CFG.suffixes.qq_info).resolve()


def available_work_dirs() -> str:
    """
    Return the supported work-directory types for the detected batch system.

    The batch system is determined using the `QQ_BATCH_SYSTEM` environment
    variable or by automatic detection. The supported work-directory types are
    returned as a comma-separated string formatted for display in help text.

    Returns:
        str: A comma-separated list of supported work directory types, each
        wrapped in quotes.
    """
    from qq_lib.batch.interface.meta import BatchMeta

    try:
        batch_system = BatchMeta.fromEnvVarOrGuess()
        work_dirs = batch_system.getSupportedWorkDirTypes()
        return ", ".join([f"'{work_dir_type}'" for work_dir_type in work_dirs])
    except QQError:
        return "??? (no batch system detected)"
