# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path

import click

from qq_lib.common import QQ_SUFFIXES, get_files_with_suffix, get_info_file
from qq_lib.error import QQError
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger
from qq_lib.states import QQState

logger = get_logger(__name__)


@click.command()
@click.option("--force", is_flag=True, help="Clear directory with an active job.")
def clear(force: bool = False):
    """
    Delete all qq run files in a directory.
    """
    try:
        clearer = QQClearer(Path())
        files = clearer.getQQFiles()
        clearer.clearFiles(files, force)
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


class QQClearer:
    def __init__(self, directory: Path):
        self.directory = directory

    def getQQFiles(self) -> list[Path]:
        files = []
        for suffix in QQ_SUFFIXES:
            files.extend(get_files_with_suffix(self.directory, suffix))

        return files

    def clearFiles(self, files: list[Path], force: bool):
        if len(files) == 0:
            return

        if self._shouldClear(force):
            for file in files:
                logger.debug(f"Removing file '{file}'.")
                Path.unlink(file)
            logger.info(
                f"Removed {len(files)} qq run file{'s' if len(files) != 1 else ''}."
            )
        else:
            raise QQError(
                "Clearing this qq job directory may corrupt or delete useful data. Use 'qq clear --force' if sure."
            )

    def _shouldClear(self, force: bool) -> bool:
        if force:
            return True

        try:
            info_file = get_info_file(self.directory)
            info = QQInformer.loadFromFile(info_file)
            state = info.getRealState()
            logger.debug(f"Job state: {str(state)}.")

            return state in [
                QQState.KILLED,
                QQState.FAILED,
                QQState.IN_AN_INCONSISTENT_STATE,
            ]
        except QQError:
            # if this fails, we know we are not in a valid qq directory,
            # so we clear everything
            return True
