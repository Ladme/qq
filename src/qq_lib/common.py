# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from pathlib import Path

import readchar
from rich.live import Live
from rich.text import Text

from qq_lib.batch import QQBatchInterface
from qq_lib.error import QQError
from qq_lib.logger import get_logger
from qq_lib.supported_batch_systems import BATCH_SYSTEMS

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
    info_files = get_files_with_suffix(current_directory, ".qqinfo")
    logger.debug(f"Detected the following qq info files: {info_files}.")
    if len(info_files) == 0:
        raise QQError("No qq job info file found.")
    if len(info_files) > 1:
        raise QQError(f"Multiple ({len(info_files)}) qq job info files detected.")

    return info_files[0]


def convert_to_batch_system(name: str) -> type[QQBatchInterface]:
    """
    Converts the name of the batch system to
    the actual type of the batch system used.

    Raises KeyError if the name is not recognized.
    """
    return BATCH_SYSTEMS[name]


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
