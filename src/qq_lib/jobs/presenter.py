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
from qq_lib.core.constants import (
    DATE_FORMAT,
    JOBS_PRESENTER_MAIN_COLOR,
    JOBS_PRESENTER_MILD_WARNING_COLOR,
    JOBS_PRESENTER_SECONDARY_COLOR,
    JOBS_PRESENTER_STRONG_WARNING_COLOR,
)
from qq_lib.properties.states import BatchState


class QQJobsPresenter:
    def __init__(self, jobs: list[BatchJobInfoInterface]):
        self._jobs = jobs
        self._stats = QQJobsStatistics()

    def createJobsInfoPanel(self, console: Console | None = None) -> Group:
        console = console or Console()
        panel_width = console.size.width

        jobs_panel = self._createBasicJobsTable()
        stats_panel = self._stats.createStatsPanel()

        content = Group(
            jobs_panel,
            Text(""),
            stats_panel,
        )

        panel = Panel(
            content,
            title=Text("COLLECTED JOBS", style="bold", justify="center"),
            border_style="white",
            padding=(1, 1),
            width=panel_width,
            expand=False,
        )

        return Group(Text(""), panel, Text(""))

    def dumpYaml(self):
        for job in self._jobs:
            print(job.toYaml())

    def _createBasicJobsTable(self) -> Table:
        table = Table(box=None, padding=(0, 1), expand=False)
        for property in [
            "S",
            "Job ID",
            "User",
            "Job Name",
            "Queue",
            "NCPUs",
            "NGPUs",
            "NNodes",
            "Times",
            "Node",
            "%CPU",
            "%Mem",
            "Exit",
        ]:
            table.add_column(
                justify="center",
                header=Text(property, style="bold", justify="center"),
            )

        for job in self._jobs:
            state = job.getJobState()
            start_time = job.getStartTime() or job.getSubmissionTime()
            if state not in {BatchState.FINISHED, BatchState.FAILED}:
                end_time = datetime.now()
            else:
                # if completion time is not available, use the last modification time
                end_time = job.getCompletionTime() or job.getModificationTime()

            cpus = job.getNCPUs()
            gpus = job.getNGPUs()
            nodes = job.getNNodes()

            self._stats.addJob(state, cpus, gpus, nodes)

            table.add_row(
                Text(state.toCode(), style=state.color),
                QQJobsPresenter._mainColorText(
                    QQJobsPresenter._shortenJobId(job.getJobId())
                ),
                QQJobsPresenter._mainColorText(job.getUser()),
                QQJobsPresenter._mainColorText(job.getJobName()),
                QQJobsPresenter._mainColorText(job.getQueue()),
                QQJobsPresenter._mainColorText(str(cpus)),
                QQJobsPresenter._mainColorText(str(gpus)),
                QQJobsPresenter._mainColorText(str(nodes)),
                QQJobsPresenter._formatTime(
                    state, start_time, end_time, job.getWalltime()
                ),
                QQJobsPresenter._formatNodesOrComment(state, job),
                QQJobsPresenter._formatUtilCPU(job.getUtilCPU()),
                QQJobsPresenter._formatUtilMem(job.getUtilMem()),
                QQJobsPresenter._formatExitCode(job.getExitCode()),
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
                return Text(end_time.strftime(DATE_FORMAT), style=state.color)
            case (
                BatchState.HELD
                | BatchState.QUEUED
                | BatchState.WAITING
                | BatchState.MOVING
            ):
                return Text(
                    format_duration_wdhhmmss(end_time - start_time),
                    style=state.color,
                )
            case BatchState.RUNNING | BatchState.EXITING:
                run_time = end_time - start_time
                return Text(
                    format_duration_wdhhmmss(run_time),
                    style=JOBS_PRESENTER_STRONG_WARNING_COLOR
                    if run_time > walltime
                    else state.color,
                ) + QQJobsPresenter._mainColorText(
                    f" / {format_duration_wdhhmmss(walltime)}"
                )

        return Text("")

    @staticmethod
    def _formatUtilCPU(util: int | None) -> Text:
        if util is None:
            return Text("")

        if util > 100:
            color = JOBS_PRESENTER_STRONG_WARNING_COLOR
        elif util >= 80:
            color = JOBS_PRESENTER_MAIN_COLOR
        elif util >= 60:
            color = JOBS_PRESENTER_MILD_WARNING_COLOR
        else:
            color = JOBS_PRESENTER_STRONG_WARNING_COLOR

        return Text(str(util), style=color)

    @staticmethod
    def _formatUtilMem(util: int | None) -> Text:
        if util is None:
            return Text("")

        if util < 90:
            color = JOBS_PRESENTER_MAIN_COLOR
        elif util < 100:
            color = JOBS_PRESENTER_MILD_WARNING_COLOR
        else:
            color = JOBS_PRESENTER_STRONG_WARNING_COLOR

        return Text(str(util), style=color)

    @staticmethod
    def _formatExitCode(exit_code: int | None) -> Text:
        if exit_code is None:
            return Text("")

        if exit_code == 0:
            return Text(str(exit_code), style=JOBS_PRESENTER_MAIN_COLOR)

        return Text(str(exit_code), style=JOBS_PRESENTER_STRONG_WARNING_COLOR)

    @staticmethod
    def _formatNodesOrComment(state: BatchState, job: BatchJobInfoInterface) -> Text:
        if nodes := job.getShortNodes():
            return QQJobsPresenter._mainColorText(
                " + ".join(nodes),
            )

        if state in {BatchState.FINISHED, BatchState.FAILED}:
            return Text("")

        if estimated := job.getJobEstimated():
            return Text(
                f"{estimated[1]} within {format_duration_wdhhmmss(estimated[0] - datetime.now()).rsplit(':', 1)[0]}",
                style=state.color,
            )

        return Text("")

    @staticmethod
    def _shortenJobId(job_id: str) -> str:
        return job_id.split(".", 1)[0]

    @staticmethod
    def _mainColorText(string: str, bold: bool = False) -> Text:
        return Text(
            string, style=f"{JOBS_PRESENTER_MAIN_COLOR} {'bold' if bold else ''}"
        )

    @staticmethod
    def _secondaryColorText(string: str, bold: bool = False) -> Text:
        return Text(
            string, style=f"{JOBS_PRESENTER_SECONDARY_COLOR} {'bold' if bold else ''}"
        )


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

        line.append(
            QQJobsPresenter._secondaryColorText(f"\n\n Jobs{spacing}", bold=True)
        )

        total = 0
        for state in BatchState:
            if state in self.n_jobs:
                count = self.n_jobs[state]
                total += count
                line.append(f"{state.toCode()} ", style=f"{state.color} bold")
                line.append(QQJobsPresenter._secondaryColorText(str(count)))
                line.append(spacing)

        line.append("Σ ", style="white bold")
        line.append(QQJobsPresenter._secondaryColorText(str(total)))
        line.append(spacing)

        return line

    def _createResourcesStatsTable(self) -> Table:
        table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))

        table.add_column("", justify="left")
        table.add_column(QQJobsPresenter._secondaryColorText("CPUs"), justify="center")
        table.add_column(QQJobsPresenter._secondaryColorText("GPUs"), justify="center")
        table.add_column(QQJobsPresenter._secondaryColorText("Nodes"), justify="center")

        table.add_row(
            QQJobsPresenter._secondaryColorText("Requested", bold=True),
            QQJobsPresenter._secondaryColorText(str(self.n_requested_cpus)),
            QQJobsPresenter._secondaryColorText(str(self.n_requested_gpus)),
            QQJobsPresenter._secondaryColorText(str(self.n_requested_nodes)),
        )
        table.add_row(
            QQJobsPresenter._secondaryColorText("Allocated", bold=True),
            QQJobsPresenter._secondaryColorText(str(self.n_allocated_cpus)),
            QQJobsPresenter._secondaryColorText(str(self.n_allocated_gpus)),
            QQJobsPresenter._secondaryColorText(str(self.n_allocated_nodes)),
        )

        return table
