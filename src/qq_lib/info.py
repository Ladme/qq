# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Self

import click
import yaml
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

import qq_lib
from qq_lib.batch import QQBatchInterface
from qq_lib.common import convert_to_batch_system, get_info_file
from qq_lib.error import QQError
from qq_lib.logger import get_logger
from qq_lib.resources import QQResources
from qq_lib.states import BatchState, NaiveState, QQState

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logger = get_logger(__name__)


@click.command(help="Get information about the qq job.")
def info():
    """
    Get information about the qq job submitted from this directory.
    """
    try:
        info_file = get_info_file(Path())
        informer = QQInformer.loadFromFile(info_file)
        console = Console()
        panel = informer.getJobStatusPanel(console)
        console.print(panel)
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


class QQInformer:
    """
    Handles collecting, loading and printing information
    about the qq run.
    """

    def __init__(
        self, batch_system: type[QQBatchInterface], info: dict[str, Any] | None = None
    ):
        self.batch_system = batch_system
        self.batch_info = None

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
        time: datetime,
        jobid: str,
    ):
        _ = resources

        self.info["batch_system"] = self.batch_system.envName()
        self.info["qq_version"] = qq_lib.__version__
        self.info["job_name"] = job_name
        self.info["job_type"] = job_type
        self.info["input_machine"] = input_machine
        self.info["job_dir"] = str(job_dir)
        self.info["excluded_files"] = (
            [str(x) for x in excluded_files] if excluded_files else None
        )

        self.info["job_state"] = str(NaiveState.QUEUED)
        self.info["submission_time"] = time.strftime(DATE_FORMAT)
        self.info["job_id"] = jobid

    def setRunning(self, time: datetime, main_node: str, work_dir: Path):
        self.info["job_state"] = str(NaiveState.RUNNING)
        self.info["start_time"] = time.strftime(DATE_FORMAT)
        self.info["main_node"] = main_node
        self.info["work_dir"] = str(work_dir)

    def setFinished(self, time: datetime):
        self.info["job_state"] = str(NaiveState.FINISHED)
        self.info["completion_time"] = time.strftime(DATE_FORMAT)
        self.info["job_exit_code"] = 0

    def setFailed(self, time: datetime, return_code: int):
        self.info["job_state"] = str(NaiveState.FAILED)
        self.info["completion_time"] = time.strftime(DATE_FORMAT)
        self.info["job_exit_code"] = return_code

    def setKilled(self, time: datetime):
        self.info["job_state"] = str(NaiveState.KILLED)
        self.info["completion_time"] = time.strftime(DATE_FORMAT)

    def exportToConsole(self):
        print("\nqq job info\n")
        print(self._convertToYaml())
        print()

    def exportToFile(self, file: Path):
        logger.debug(f"Exporting qq info into '{file}'.")
        with Path.open(file, "w") as output:
            output.write("# qq job info file\n")
            output.write(self._convertToYaml())
            output.write("\n")

    def getJobStatusPanel(self, console: Console | None = None) -> Group:
        (message, details, state) = self._getStateMessages()

        console = console or Console()
        term_width = console.size.width
        panel_width = max(60, term_width // 3)

        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column(justify="right", style="bold")
        table.add_column(justify="left")

        table.add_row("Job state:", Text(message, style=f"{state.color} bold"))
        if details.strip():
            table.add_row("", Text(details, style="white"))

        panel = Panel(
            table,
            title=Text(f"JOB: {self.getJobId()}", style="bold", justify="center"),
            border_style="white",
            padding=(1, 2),
            width=panel_width,
        )

        return Group(Text(""), panel, Text(""))

    @classmethod
    def loadFromFile(cls, file: Path) -> Self:
        logger.debug(f"Loading qq info from '{file}'.")
        with Path.open(file) as input:
            info: dict = yaml.safe_load(input)

        if not isinstance(info, dict):
            raise QQError(f"Could not read the qqinfo file '{file}'.")

        try:
            batch_system_string = info["batch_system"]
        except KeyError:
            raise QQError(
                "Undefined batch system: 'batch_system' missing from qqinfo file."
            )

        try:
            batch_system = convert_to_batch_system(batch_system_string)
        except KeyError:
            raise QQError(
                f"Unknown batch system: '{batch_system_string}' does not match any known batch system."
            )

        return cls(batch_system, info)

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

    def getJobId(self) -> str:
        jobid = self.info.get("job_id")

        if not jobid:
            raise QQError("Property 'job_id' not available in qq info.")

        return jobid

    def getJobName(self) -> str:
        job_name = self.info.get("job_name")

        if not job_name:
            raise QQError("Property 'job_name' not available in qq info.")

        return job_name

    def getNaiveState(self) -> NaiveState:
        """
        Get the naive state of the job as defined in the qqinfo file.
        """
        state = self.info.get("job_state")
        if not state:
            return NaiveState.UNKNOWN

        return NaiveState.fromStr(state)

    def getBatchState(self) -> BatchState:
        """
        Get the state of the job according to the batch system.
        """
        if not self.batch_info:
            self.batch_info = self.batch_system.getJobInfo(self.getJobId())

        state_code = self.batch_info.get(self.batch_system.jobState())
        logger.debug(f"Batch state code: '{state_code}'")
        if not state_code:
            return BatchState.UNKNOWN

        return self.batch_system.translateJobState(state_code)

    def getRealState(self) -> QQState:
        """
        Get the real state of the job as understood from the qqinfo and the batch system.
        """
        naive = self.getNaiveState()
        batch = self.getBatchState()
        return QQState.fromStates(naive, batch)

    def _convertToYaml(self) -> str:
        cleaned = {k: v for k, v in self.info.items() if v is not None}

        return yaml.dump(cleaned, default_flow_style=False, sort_keys=False)

    def _getStateMessages(self) -> tuple[str, str, QQState]:
        submission_time = self.info.get("submission_time")
        if not submission_time:
            raise QQError(
                f"Submission time '{submission_time}' is not available in qq info."
            )
        start_time = self.info.get("start_time")
        completion_time = self.info.get("completion_time")

        if start_time:
            start = datetime.strptime(start_time, DATE_FORMAT)
            if completion_time:
                end = datetime.strptime(completion_time, DATE_FORMAT)
            else:
                end = datetime.now()
        else:
            start = datetime.strptime(submission_time, DATE_FORMAT)
            if completion_time:
                end = datetime.strptime(completion_time, DATE_FORMAT)
            else:
                end = datetime.now()

        real_state = self.getRealState()
        (message, details) = real_state.info(
            start, end, self.info.get("job_exit_code"), self.info.get("main_node")
        )
        return (message, details, real_state)
