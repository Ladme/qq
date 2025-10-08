# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime, timedelta

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from qq_lib.batch.interface import BatchJobInfoInterface
from qq_lib.core.common import (
    format_duration_wdhhmmss,
)
from qq_lib.core.constants import DATE_FORMAT
from qq_lib.properties.states import BatchState


class QQJobsPresenter:
    def __init__(self, jobs: list[BatchJobInfoInterface]):
        self._jobs = jobs

    def createBasicJobsPanel(self, console: Console | None = None) -> Group:
        console = console or Console()
        panel_width = max(120, 2 * console.size.width // 3)

        panel = Panel(
            self._createBasicJobsTable(),
            title=Text("List of collected jobs", style="bold", justify="center"),
            border_style="white",
            padding=(1, 1),
            width=panel_width,
            expand=False,
        )

        return Group(Text(""), panel, Text(""))

    def _createBasicJobsTable(self) -> Table:
        table = Table(box=None, padding=(0, 1))
        for property in [
            "State",
            "Job ID",
            "User",
            "Job Name",
            "Queue",
            "CPUs",
            "GPUs",
            "Nodes",
            "Time",
            "%Util CPU",
            "%Util Mem",
            "Exit",
        ]:
            table.add_column(
                justify="center", header=Text(property, style="bold", justify="center")
            )

        for job in self._jobs:
            state = job.getJobState()
            start_time = job.getStartTime() or job.getSubmissionTime()
            end_time = job.getCompletionTime() or datetime.now()

            cpu_util = job.getUtilCPU()
            mem_util = job.getUtilMem()
            exit = job.getExitCode()

            table.add_row(
                Text(state.toCode(), style=state.color),
                QQJobsPresenter._shortenJobId(job.getJobId()),
                job.getUser(),
                job.getJobName(),
                job.getQueue(),
                str(job.getNCPUs()),
                str(job.getNGPUs()),
                str(job.getNNodes()),
                QQJobsPresenter._formatTime(
                    state, start_time, end_time, job.getWalltime()
                ),
                Text(str(cpu_util), style="bright_red" if cpu_util < 50 else "default")
                if cpu_util is not None
                else "",
                Text(str(mem_util), style="bright_red" if mem_util > 90 else "default")
                if mem_util is not None
                else "",
                Text(str(exit), style="bright_red" if exit != 0 else "default")
                if exit is not None
                else "",
            )

        return table

    @staticmethod
    def _formatTime(
        state: BatchState, start_time: datetime, end_time: datetime, walltime: timedelta
    ) -> Text:
        match state:
            case BatchState.UNKNOWN | BatchState.SUSPENDED:
                return Text("")
            case BatchState.FAILED | BatchState.FINISHED:
                return Text(end_time.strftime(DATE_FORMAT))
            case (
                BatchState.HELD
                | BatchState.QUEUED
                | BatchState.WAITING
                | BatchState.MOVING
            ):
                return Text(format_duration_wdhhmmss(end_time - start_time))
            case BatchState.RUNNING | BatchState.EXITING:
                run_time = end_time - start_time
                return Text(
                    format_duration_wdhhmmss(run_time),
                    style="bright_red" if run_time > walltime else "default",
                ) + Text(f" / {format_duration_wdhhmmss(walltime)}", style="grey70")

        return Text("")

    @staticmethod
    def _shortenJobId(job_id: str) -> str:
        split = job_id.split(".", maxsplit=1)
        if len(split) == 1:
            return split[0]

        return split[0] + split[1][0].upper()
