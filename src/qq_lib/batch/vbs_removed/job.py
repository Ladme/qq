# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import getpass
import socket
from datetime import datetime, timedelta
from pathlib import Path

from qq_lib.batch.interface import BatchJobInfoInterface
from qq_lib.properties.size import Size
from qq_lib.properties.states import BatchState

from .system import VirtualJob


class VBSJobInfo(BatchJobInfoInterface):
    """
    Implementation of BatchJobInterface for VBS.
    """

    def __init__(self, job: VirtualJob | None):
        # store a reference to the virtual job
        self._job = job

    def isEmpty(self) -> bool:
        return False

    def update(self) -> None:
        # does nothing
        pass

    def getId(self) -> str:
        return self._job.job_id

    def getState(self) -> BatchState:
        if self._job:
            return self._job.state

        return BatchState.UNKNOWN

    def getComment(self) -> str | None:
        return None

    def getEstimated(self) -> tuple[datetime, str] | None:
        return None

    def getMainNode(self) -> str | None:
        if not self._job:
            return None

        if node := self._job.node:
            return str(node)

        return None

    def getNodes(self) -> list[str] | None:
        # only single-node jobs
        if node := self.getMainNode():
            return [node]

        return None

    def getShortNodes(self) -> list[str] | None:
        return self.getNodes()

    def getName(self) -> str:
        return self._job.script

    def getNCPUs(self) -> int:
        return 1

    def getNGPUs(self) -> int:
        return 0

    def getNNodes(self) -> int:
        return 1

    def getMem(self) -> Size:
        # arbitrary size
        return Size(1, "gb")

    def getStartTime(self) -> datetime | None:
        if self._job.state == BatchState.QUEUED:
            return None

        # arbitrary time
        return datetime.now()

    def getSubmissionTime(self) -> datetime:
        # arbitrary time
        return datetime.now()

    def getCompletionTime(self) -> datetime | None:
        if self._job.state not in {BatchState.FINISHED, BatchState.FAILED}:
            return None

        # arbitrary time
        return datetime.now()

    def getModificationTime(self) -> datetime:
        # arbitrary time
        return datetime.now()

    def getUser(self) -> str:
        return getpass.getuser()

    def getWalltime(self) -> timedelta:
        # arbitrary walltime
        return timedelta(hours=24)

    def getQueue(self) -> str:
        return "default"

    def getUtilCPU(self) -> int | None:
        return 100

    def getUtilMem(self) -> int | None:
        return 100

    def getExitCode(self) -> int | None:
        if self._job.state == BatchState.FINISHED:
            return 0
        if self._job.state == BatchState.FAILED:
            return 1

        return None

    def getInputDir(self) -> Path:
        # arbitrary path
        return Path()

    def getInputMachine(self) -> str:
        return socket.gethostname()

    def getInfoFile(self) -> Path:
        # arbitrary path
        return Path()

    def toYaml(self) -> str:
        return ""
