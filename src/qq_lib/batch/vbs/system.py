# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import shutil
import signal
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path

from qq_lib.properties.states import BatchState


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
        if job.state in {BatchState.FINISHED, BatchState.FAILED}:
            raise VBSError(f"Job '{job_id}' is completed.")

        if job.state == BatchState.RUNNING and job.process:
            sig = signal.SIGKILL if hard else signal.SIGTERM
            job.process.send_signal(sig)

        job.state = BatchState.FAILED
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
        if job.process and job.process.returncode == 0:
            job.state = BatchState.FINISHED
        else:
            job.state = BatchState.FAILED
        job.process = None
