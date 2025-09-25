# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

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
    if len(info_files) > 1:
        raise QQError("Multiple qq job info files found.")

    return info_files[0]


def get_info_files(directory: Path) -> list[Path]:
    """
    Retrieve all qq job info files in a directory.

    This function searches for files matching the `QQ_INFO_SUFFIX` in the
    provided directory. It raises an error if no info files are found.

    Args:
        directory (Path): The directory to search in.

    Returns:
        list[Path]: A list of Path objects representing the detected qq job info files.

    Raises:
        QQError: If no info files are found in the directory.
    """
    info_files = get_files_with_suffix(directory, QQ_INFO_SUFFIX)
    logger.debug(f"Detected the following qq info files: {info_files}.")
    if len(info_files) == 0:
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
