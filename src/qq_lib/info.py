# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import datetime
from pathlib import Path
from typing import Any, Self

import click
import yaml

from qq_lib.error import QQError
from qq_lib.resources import QQResources

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


@click.command()
def info():
    """
    Get information about the qq job submitted from this directory.
    """
    pass


class QQInformer:
    """
    Handles collecting, loading and printing information
    about the qq run.
    """

    def __init__(self, info: dict[str, Any] | None = None):
        if info:
            self.info = info
        else:
            self.info = {}

    def setSubmitted(
        self,
        job_name: str,
        job_type: str,
        input_machine: str,
        job_dir: Path,
        resources: QQResources,
        excluded_files: list[Path] | None,
        time: datetime.datetime,
        jobid: str,
    ):
        self.info["job_name"] = job_name
        self.info["job_type"] = job_type
        self.info["input_machine"] = input_machine
        self.info["job_dir"] = str(job_dir)
        self.info["excluded_files"] = (
            [str(x) for x in excluded_files] if excluded_files else None
        )

        self.info["job_state"] = "queued"
        self.info["submission_time"] = time.strftime(DATE_FORMAT)
        self.info["job_id"] = jobid

    def setRunning(self, time: datetime.datetime, main_node: str, work_dir: Path):
        self.info["job_state"] = "running"
        self.info["start_time"] = time.strftime(DATE_FORMAT)
        self.info["main_node"] = main_node
        self.info["work_dir"] = str(work_dir)

    def setFinished(self, time: datetime.datetime):
        self.info["job_state"] = "finished"
        self.info["completion_time"] = time.strftime(DATE_FORMAT)
        self.info["job_exit_code"] = 0

    def setFailed(self, time: datetime.datetime, return_code: int):
        self.info["job_state"] = "failed"
        self.info["completion_time"] = time.strftime(DATE_FORMAT)
        self.info["job_exit_code"] = return_code

    def setKilled(self, time: datetime.datetime):
        self.info["job_state"] = "killed"
        self.info["completion_time"] = time.strftime(DATE_FORMAT)

    def exportToConsole(self):
        print("\nqq job info\n")
        print(self._exportToYaml())
        print()

    def exportToFile(self, file: Path):
        with open(file, "w") as output:
            output.write("# qq job info file\n")
            output.write(self._exportToYaml())
            output.write("\n")

    @classmethod
    def loadFromFile(cls, file: Path) -> Self:
        with open(file) as input:
            info = yaml.safe_load(input)

        return cls(info)

    def getDestination(self) -> tuple[str, str] | None:
        """
        Returns the main node and working directory for the job.
        If not set up, returns None.
        """
        main_node = self.info.get("main_node")
        work_dir = self.info.get("work_dir")

        if not main_node or not work_dir:
            return None

        return (main_node, work_dir)

    def getState(self) -> str:
        state = self.info.get("job_state")
        if not state:
            return "unknown"

        return state

    def getJobId(self) -> str:
        jobid = self.info.get("job_id")

        if not jobid:
            raise QQError("Property 'job_id' not available in qq info")

        return jobid

    def _exportToYaml(self) -> str:
        cleaned = {k: v for k, v in self.info.items() if v is not None}

        return yaml.dump(cleaned, default_flow_style=False, sort_keys=False)
