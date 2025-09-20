# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from pathlib import Path
from time import sleep

import click

from qq_lib.common import get_info_file
from qq_lib.error import QQError
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger

logger = get_logger(__name__)


@click.command()
def go():
    """
    Go to the working directory of the qq job submitted from this directory.
    """
    try:
        goer = QQGoer(Path())
        goer.navigate()
    except QQError as e:
        logger.error(e)
        sys.exit(1)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(1)


class QQGoer:
    def __init__(self, current_dir: Path):
        self.info_file = get_info_file(current_dir)
        self.info = QQInformer.loadFromFile(self.info_file)
        self.batch_system = self.info.batch_system

        self.state = self.info.getState()
        destination = self._getDestination()
        if destination:
            (self.host, self.directory) = destination
        else:
            self.host = None
            self.directory = None

    def navigate(self):
        if self.state == "finished" or self.state == "failed":
            logger.warning("Job has finished: working directory may no longer exist.")
        elif self.state == "killed":
            logger.warning("Job has been killed: working directory may not exist.")
        elif self.state == "queued":
            logger.warning(
                "Job is queued: working directory does not yet exist. Will retry every 5 seconds."
            )
            # keep retrying until the job gets run
            while self.state == "queued":
                sleep(5)
                self.info = QQInformer.loadFromFile(self.info_file)
                self.state = self.info.getState()
                destination = self._getDestination()
                if destination:
                    (self.host, self.directory) = destination

        elif self.state == "running":
            pass
        else:
            logger.warning("Job is in an unknown, unrecognized, or inconsistent state.")

        if not self.directory or not self.host:
            raise QQError(
                "Host ('main_node') or working directory ('work_dir') are not defined in the qqinfo file."
            )

        logger.info(f"Navigating to '{self.directory}' on '{self.host}'.")
        try:
            result = self.batch_system.navigateToDestination(self.host, self.directory)
            if result.returncode != 0:
                raise Exception
        except KeyboardInterrupt:
            pass
        except Exception as e:
            if str(e) != "":
                raise QQError(
                    f"Could not reach '{self.host}:{self.directory}': {e}."
                ) from e
            raise QQError(f"Could not reach '{self.host}:{self.directory}'.") from e

    def _getDestination(self) -> tuple[str, str] | None:
        destination = self.info.getDestination()
        if destination:
            logger.debug(f"Destination is {destination}.")
        else:
            logger.debug("Destination is not specified.")

        return destination
