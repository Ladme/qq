# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from dataclasses import dataclass, field
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
        self._stats = QQJobsStatistics()

    def createJobsInfoPanel(self, console: Console | None = None) -> Group:
        console = console or Console()
        panel_width = max(120, 2 * console.size.width // 3)

        jobs_panel = self._createBasicJobsTable()
        stats_panel = self._stats.createStatsPanel()

        content = Group(
            jobs_panel,
            Text(""),
            # Rule(style="grey70", characters="- "),
            # Text(""),
            stats_panel,
        )

        panel = Panel(
            content,
            title=Text("List of collected jobs", style="bold", justify="center"),
            border_style="white",
            padding=(1, 1),
            width=panel_width,
            expand=False,
        )

        return Group(Text(""), panel, Text(""))

    def _createBasicJobsTable(self) -> Table:
        table = Table(box=None, padding=(0, 1), expand=False)
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

            cpus = job.getNCPUs()
            gpus = job.getNGPUs()
            nodes = job.getNNodes()

            self._stats.addJob(state, cpus, gpus, nodes)

            cpu_util = job.getUtilCPU()
            # TODO: move into separate function
            if cpu_util:
                if cpu_util < 60:
                    cpu_color = "bright_red"
                elif cpu_util < 80:
                    cpu_color = "bright_yellow"
                else:
                    cpu_color = "white"

            mem_util = job.getUtilMem()
            # TODO: move into separate function
            if mem_util:
                if mem_util < 90:
                    mem_color = "white"
                elif mem_util < 100:
                    mem_color = "bright_yellow"
                else:
                    mem_color = "bright_red"
            exit = job.getExitCode()

            table.add_row(
                Text(state.toCode(), style=state.color),
                Text(QQJobsPresenter._shortenJobId(job.getJobId()), style="white"),
                Text(job.getUser(), style="white"),
                Text(job.getJobName(), style="white"),
                Text(job.getQueue(), style="white"),
                Text(str(cpus), style="white"),
                Text(str(gpus), style="white"),
                Text(str(nodes), style="white"),
                QQJobsPresenter._formatTime(
                    state, start_time, end_time, job.getWalltime()
                ),
                Text(str(cpu_util), style=cpu_color) if cpu_util is not None else "",
                Text(str(mem_util), style=mem_color) if mem_util is not None else "",
                Text(str(exit), style="bright_red" if exit != 0 else "white")
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
                return Text(end_time.strftime(DATE_FORMAT), style="white")
            case (
                BatchState.HELD
                | BatchState.QUEUED
                | BatchState.WAITING
                | BatchState.MOVING
            ):
                return Text(
                    format_duration_wdhhmmss(end_time - start_time), style="white"
                )
            case BatchState.RUNNING | BatchState.EXITING:
                run_time = end_time - start_time
                return Text(
                    format_duration_wdhhmmss(run_time),
                    style="bright_red" if run_time > walltime else "white",
                ) + Text(f" / {format_duration_wdhhmmss(walltime)}", style="white")

        return Text("")

    @staticmethod
    def _shortenJobId(job_id: str) -> str:
        split = job_id.split(".", maxsplit=1)
        if len(split) == 1:
            return split[0]

        return split[0] + split[1][0].upper()


@dataclass
class QQJobsStatistics:
    """
    Dataclass for collecting statistics about jobs.
    """

    # Number of jobs of various types.
    n_jobs: dict[BatchState, int] = field(default_factory=dict)

    # Number of requested CPUs.
    n_requested_cpus: int = 0

    # Number of allocated CPUs.
    n_allocated_cpus: int = 0

    # Number of requested GPUs.
    n_requested_gpus: int = 0

    # Number of allocated GPUs.
    n_allocated_gpus: int = 0

    # Number of requested nodes.
    n_requested_nodes: int = 0

    # Number of allocated nodes.
    n_allocated_nodes: int = 0

    def addJob(self, state: BatchState, cpus: int, gpus: int, nodes: int):
        try:
            self.n_jobs[state] += 1
        except KeyError:
            self.n_jobs[state] = 1

        if state in {BatchState.QUEUED, BatchState.HELD}:
            self.n_requested_cpus += cpus
            self.n_requested_gpus += gpus
            self.n_requested_nodes += nodes

        if state in {BatchState.RUNNING, BatchState.EXITING}:
            self.n_allocated_cpus += cpus
            self.n_allocated_gpus += gpus
            self.n_allocated_nodes += nodes

    def createStatsPanel(self) -> Group:
        jobs_text = self._createJobStatesStats()
        resources_table = self._createResourcesStatsTable()

        table = Table.grid(expand=False)
        table.add_column(justify="left")
        # spacer column
        table.add_column(justify="center", width=5)
        table.add_column(justify="right")

        table.add_row(jobs_text, "", resources_table)

        return Group(table)

    def _createJobStatesStats(self) -> Text:
        spacing = "    "
        line = Text(spacing)

        line.append(f"\n\n Jobs{spacing}", style="grey70 bold")

        total = 0
        for state in BatchState:
            if state in self.n_jobs:
                count = self.n_jobs[state]
                total += count
                line.append(f"{state.toCode()} ", style=f"{state.color} bold")
                line.append(f"{count}", style="grey70")
                line.append(spacing)

        line.append("Σ ", style="white bold")
        line.append(f"{total}", style="grey70")
        line.append(spacing)

        return line

    def _createResourcesStatsTable(self) -> Table:
        table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))

        table.add_column("", justify="left")
        table.add_column(Text("CPUs", style="grey70"), justify="center")
        table.add_column(Text("GPUs", style="grey70"), justify="center")
        table.add_column(Text("Nodes", style="grey70"), justify="center")

        table.add_row(
            Text("Requested", style="bold grey70"),
            Text(str(self.n_requested_cpus), style="grey70"),
            Text(str(self.n_requested_gpus), style="grey70"),
            Text(str(self.n_requested_nodes), style="grey70"),
        )
        table.add_row(
            Text("Allocated", style="bold grey70"),
            Text(str(self.n_allocated_cpus), style="grey70"),
            Text(str(self.n_allocated_gpus), style="grey70"),
            Text(str(self.n_allocated_nodes), style="grey70"),
        )

        return table
