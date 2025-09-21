# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import socket
import sys
from pathlib import Path
from time import sleep

import click
from rich.console import Console

from qq_lib.common import get_info_file
from qq_lib.error import QQError
from qq_lib.info import QQInformer
from qq_lib.logger import get_logger
from qq_lib.states import QQState

logger = get_logger(__name__)
console = Console()


@click.command()
def go():
    """
    Go to the working directory of the qq job submitted from this directory.
    """
    try:
        goer = QQGoer(Path())
        goer.printInfo()
        goer.navigate()
        print()
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        print()
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


class QQGoer:
    def __init__(self, current_dir: Path):
        self.info_file = get_info_file(current_dir)
        self.info = QQInformer.loadFromFile(self.info_file)
        self.batch_system = self.info.batch_system

        self.state = self.info.getRealState()
        destination = self._getDestination()
        if destination:
            (self.host, self.directory) = destination
        else:
            self.host = None
            self.directory = None

    def printInfo(self):
        panel = self.info.getJobStatusPanel()
        console.print(panel)

    def navigate(self):
        if self._isInWorkDir():
            logger.info("You are already in the working directory.")
            return

        if self.state in [QQState.FINISHED]:
            raise QQError(
                "Job has finished and was synchronized: working directory does not exist."
            )
        if self.state in [QQState.FAILED]:
            logger.warning(
                "Job has finished with an error code: working directory may no longer exist."
            )
        elif self.state == QQState.KILLED:
            logger.warning("Job has been killed: working directory may not exist.")
        elif self.state in [
            QQState.QUEUED,
            QQState.BOOTING,
            QQState.HELD,
            QQState.WAITING,
        ]:
            logger.warning(
                f"Job is {self.state}: working directory does not yet exist. Will retry every 5 seconds."
            )
            # keep retrying until the job gets run
            while self.state in [
                QQState.QUEUED,
                QQState.BOOTING,
                QQState.HELD,
                QQState.WAITING,
            ]:
                sleep(5)
                self.info = QQInformer.loadFromFile(self.info_file)
                self.state = self.info.getRealState()
                destination = self._getDestination()
                if destination:
                    (self.host, self.directory) = destination

                if self._isInWorkDir():
                    logger.info("You are already in the working directory.")
                    return

        elif self.state in [QQState.RUNNING, QQState.SUSPENDED]:
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

    def _isInWorkDir(self) -> bool:
        return (
            self.directory
            and Path(self.directory).resolve() == Path.cwd().resolve()
            and self.host == socket.gethostname()
        )

    def _getDestination(self) -> tuple[str, str] | None:
        destination = self.info.getDestination()
        if destination:
            logger.debug(f"Destination is {destination}.")
        else:
            logger.debug("Destination is not specified.")

        return destination
