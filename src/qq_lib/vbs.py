# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
This module implements a virtual batch system
used for testing and its integration with qq.
"""

import os
import shutil
import signal
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from qq_lib.batch import (
    BatchJobInfoInterface,
    QQBatchInterface,
    QQBatchMeta,
)
from qq_lib.error import QQError
from qq_lib.resources import QQResources
from qq_lib.states import BatchState


class VBSError(Exception):
    """Common exception type for Virtual Batch System errors."""

    pass


@dataclass
class VirtualJob:
    job_id: str
    script: Path
    use_scratch: bool
    state: BatchState = BatchState.QUEUED
    node: Path | None = None
    output: str = ""
    scratch: Path | None = None
    process: subprocess.Popen | None = None

    def tryCreateScratch(self):
        """Create a scratch directory on the given node."""
        if not self.use_scratch:
            return

        self.scratch = self.node / self.job_id
        try:
            self.scratch.mkdir()
        except Exception:
            raise VBSError(
                f"Could not create a scratch directory for job '{self.job_id}'."
            )


class VirtualBatchSystem:
    """
    A virtual batch system for testing purposes.
    Jobs are stored in a dictionary. Nodes are temporary directories.
    Jobs can run asynchronously on separate nodes.
    """

    def __init__(self):
        """Initialize the Virtual Batch System instance."""
        self.jobs: dict[str, VirtualJob] = {}
        self.nodes: list[Path] = []
        self._freeze_events: dict[str, threading.Event] = {}

    def __del__(self):
        """Remove the virtual nodes."""
        for node in self.nodes:
            if node.exists():
                shutil.rmtree(node)
        self.nodes.clear()

    def clearJobs(self):
        """
        Removes all jobs from the batch system (does not terminate their threads).
        """
        self.jobs.clear()

    def submitJob(self, script: Path, use_scratch: bool) -> str:
        """Register a new job in a queued state."""
        # generate a unique job id
        job_id = str(len(self.jobs))

        if job_id in self.jobs:
            raise VBSError(f"Job '{job_id}' already exists.")

        # submit the job
        self.jobs[job_id] = VirtualJob(
            job_id=job_id, script=script, use_scratch=use_scratch
        )

        return job_id

    def runJob(self, job_id: str, freeze: bool = False):
        """Assign a node to the target job and run it asynchronously."""
        job = self.jobs[job_id]
        job.node = self._createNode()
        job.tryCreateScratch()

        event = None
        if freeze:
            event = threading.Event()
            self._freeze_events[job_id] = event

        thread = threading.Thread(target=self._worker, args=(job, event), daemon=True)
        thread.start()

    def killJob(self, job_id: str, hard: bool = False):
        """Terminate a running job."""
        job = self.jobs[job_id]
        if job.state == BatchState.FINISHED:
            raise VBSError(f"Job '{job_id}' is finished.")

        if job.state == BatchState.RUNNING and job.process:
            sig = signal.SIGKILL if hard else signal.SIGTERM
            job.process.send_signal(sig)

        job.state = BatchState.FINISHED
        job.process = None

    def releaseFrozenJob(self, job_id: str):
        """Release a frozen job so it can complete."""
        if job_id not in self._freeze_events:
            raise VBSError(f"Job '{job_id}' is not frozen or does not exist.")
        self._freeze_events[job_id].set()
        del self._freeze_events[job_id]

    def _createNode(self) -> Path:
        """Create a new virtual node."""
        node = Path(tempfile.mkdtemp())
        self.nodes.append(node)

        return node

    def _worker(self, job: VirtualJob, freeze_event: threading.Event | None = None):
        """Run the script associated with target job."""
        job.state = BatchState.RUNNING

        # if freezing requested, block here until released
        if freeze_event is not None:
            freeze_event.wait()

        job.process = subprocess.Popen(
            str(job.script),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        stdout, _ = job.process.communicate()
        job.output = stdout
        job.state = BatchState.FINISHED
        job.process = None


# forward declaration
class VBSJobInfo(BatchJobInfoInterface):
    pass


class QQVBS(QQBatchInterface[VBSJobInfo], metaclass=QQBatchMeta):
    """
    Implementation of QQBatchInterface for the Virtual Batch System.
    """

    _batch_system = VirtualBatchSystem()

    def envName() -> str:
        return "VBS"

    def isAvailable() -> bool:
        # always available
        return True

    def getScratchDir(job_id: str) -> Path:
        job = QQVBS._batch_system.jobs.get(job_id)
        if not job:
            raise QQError(f"Job '{job_id}' does not exist.")

        scratch = job.scratch
        if not scratch:
            raise QQError(f"Job '{job_id}' does not have a scratch directory.")

        return scratch

    def jobSubmit(res: QQResources, _queue: str, script: Path, _job_name: str) -> str:
        try:
            return QQVBS._batch_system.submitJob(script, res.useScratch())
        except VBSError as e:
            raise QQError(f"Failed to submit script '{str(script)}': {e}.")

    def jobKill(job_id: str):
        try:
            QQVBS._batch_system.killJob(job_id)
        except VBSError as e:
            raise QQError(f"Failed to kill job '{job_id}': {e}.")

    def jobKillForce(job_id: str):
        try:
            QQVBS._batch_system.killJob(job_id, hard=True)
        except VBSError as e:
            raise QQError(f"Failed to kill job '{job_id}': {e}.")

    def navigateToDestination(host: str, directory: Path):
        try:
            os.chdir(Path(host) / directory)
        except Exception:
            raise QQError(
                f"Could not reach '{host}:{str(directory)}': Could not change directory."
            )

    def readRemoteFile(_host: str, file: Path) -> str:
        # file is always local
        try:
            return file.read_text()
        except Exception as e:
            raise QQError(f"Could not read file '{file}': {e}.") from e

    def writeRemoteFile(_host: str, file: Path, content: str):
        # file is always local
        try:
            file.write_text(content)
        except Exception as e:
            raise QQError(f"Could not write file '{file}': {e}.") from e

    def makeRemoteDir(_host: str, directory: Path):
        # always local
        try:
            directory.mkdir(exist_ok=True)
        except Exception as e:
            raise QQError(f"Could not create a directory '{directory}': {e}") from e

    def listRemoteDir(_host: str, directory: Path) -> list[Path]:
        # always local
        try:
            return list(directory.iterdir())
        except Exception as e:
            raise QQError(f"Could not list a directory '{directory}': {e}") from e

    def moveRemoteFiles(host: str, files: list[Path], moved_files: list[Path]):
        if len(files) != len(moved_files):
            raise QQError(
                "The provided 'files' and 'moved_files' must have the same length."
            )

        # always local
        try:
            for src, dst in zip(files, moved_files):
                shutil.move(str(src), str(dst))
        except Exception as e:
            raise QQError("Could not move files.") from e

    def syncWithExclusions(
        src_dir: Path,
        dest_dir: Path,
        _src_host: str | None,
        _dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ):
        # directories are always local
        QQBatchInterface.syncWithExclusions(
            src_dir, dest_dir, None, None, exclude_files
        )

    def syncSelected(
        src_dir: Path,
        dest_dir: Path,
        _src_host: str | None,
        _dest_host: str | None,
        include_files: list[Path],
    ):
        # directories are always local
        QQBatchInterface.syncSelected(src_dir, dest_dir, None, None, include_files)

    def buildResources(queue: str, **kwargs) -> QQResources:
        return QQResources.mergeResources(
            QQResources(**kwargs), QQVBS._getDefaultServerResources()
        )

    def isShared(_directory: Path) -> bool:
        # always shared
        return True

    @staticmethod
    def _getDefaultServerResources() -> QQResources:
        """
        Return a QQResources object representing the default resources for a batch job.

        Returns:
            QQResources: Default batch job resources with predefined settings.
        """
        return QQResources(
            nnodes=1,
            ncpus=1,
            mem_per_cpu="1gb",
            work_dir="scratch_local",
            work_size_per_cpu="1gb",
            walltime="1d",
        )

    def getJobInfo(job_id: str) -> VBSJobInfo:
        return VBSJobInfo(QQVBS._batch_system.jobs.get(job_id))  # ty: ignore[invalid-return-type]


# register QQVBS
QQBatchMeta.register(QQVBS)


class VBSJobInfo(BatchJobInfoInterface):
    """
    Implementation of BatchJobInterface for VBS.
    """

    def __init__(self, job: VirtualJob | None):
        # store a reference to the virtual job
        self._job = job

    def update(self):
        # does nothing
        pass

    def getJobState(self) -> BatchState:
        if self._job:
            return self._job.state

        return BatchState.UNKNOWN

    def getJobComment(self) -> str | None:
        return None

    def getJobEstimated(self) -> tuple[datetime, str] | None:
        return None
