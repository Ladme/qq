# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from pathlib import Path

from qq_lib.batch import QQBatchInterface
from qq_lib.error import QQError
from qq_lib.logger import get_logger
from qq_lib.pbs import QQPBS

logger = get_logger(__name__)

QQ_SUFFIXES = [".qqinfo", ".qqout", ".stdout", ".stderr"]
BATCH_SYSTEMS = {QQPBS.envName(): QQPBS}


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
