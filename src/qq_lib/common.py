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
    Get the list of files inside the directory with the specified suffix.
    """
    files = []
    for file in directory.iterdir():
        if file.is_file() and file.suffix == suffix:
            files.append(file)

    return files


def get_info_file(current_directory: Path) -> Path:
    info_files = get_files_with_suffix(current_directory, QQ_INFO_SUFFIX)
    logger.debug(f"Detected the following qq info files: {info_files}.")
    if len(info_files) == 0:
        raise QQError("No qq job info file found.")
    if len(info_files) > 1:
        raise QQError(f"Multiple ({len(info_files)}) qq job info files detected.")

    return info_files[0]


def yes_or_no_prompt(prompt: str) -> bool:
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
