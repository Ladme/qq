# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from tabulate import Line, TableFormat, tabulate

from qq_lib.batch.interface import BatchJobInterface
from qq_lib.core.common import (
    format_duration_wdhhmmss,
)
from qq_lib.core.config import CFG
from qq_lib.properties.states import BatchState


class QQJobsPresenter:
    """
    Present information about a collection of qq jobs and their statistics.
    """

    # Mapping of human-readable color names to ANSI escape codes.
    ANSI_COLORS = {
        # default
        "default": "",
        # standard colors
        "black": "\033[30m",
        "red": "\033[31m",
        "green": "\033[32m",
        "yellow": "\033[33m",
        "blue": "\033[34m",
        "magenta": "\033[35m",
        "cyan": "\033[36m",
        "white": "\033[37m",
        # bright colors
        "bright_black": "\033[90m",
        "bright_red": "\033[91m",
        "bright_green": "\033[92m",
        "bright_yellow": "\033[93m",
        "bright_blue": "\033[94m",
        "bright_magenta": "\033[95m",
        "bright_cyan": "\033[96m",
        "bright_white": "\033[97m",
        # other colors
        "grey90": "\033[38;5;254m",
        "grey70": "\033[38;5;249m",
        "grey50": "\033[38;5;244m",
        "grey30": "\033[38;5;239m",
        "grey10": "\033[38;5;233m",
        # bold:
        "bold": "\033[1m",
        # reset
        "reset": "\033[0m",
    }

    # Table formatting configuration for `tabulate`.
    COMPACT_TABLE = TableFormat(
        lineabove=Line("", "", "", ""),
        linebelowheader="",
        linebetweenrows="",
        linebelow=Line("", "", "", ""),
        headerrow=("", " ", ""),
        datarow=("", " ", ""),
        padding=0,
        with_header_hide=["lineabove", "linebelow"],
    )

    def __init__(self, jobs: list[BatchJobInterface]):
        """
        Initialize the presenter with a list of jobs.

        Args:
            jobs (list[BatchJobInterface]): List of job information objects
                to be presented.
        """
        self._jobs = jobs
        self._stats = QQJobsStatistics()

    def createJobsInfoPanel(self, console: Console | None = None) -> Group:
        """
        Create a Rich panel displaying job information and statistics.

        Args:
            console (Console | None): Optional Rich Console instance.
                If None, a new Console will be created.

        Returns:
            Group: Rich Group containing the jobs table and stats panel.
        """
        console = console or Console()
        panel_width = console.size.width

        # convert ANSI codes to Rich Text
        jobs_panel = Text.from_ansi(self._createBasicJobsTable())
        stats_panel = self._stats.createStatsPanel()

        content = Group(
            jobs_panel,
            Text(""),
            stats_panel,
        )

        panel = Panel(
            content,
            title=Text(
                "COLLECTED JOBS", style=CFG.jobs_presenter.title_style, justify="center"
            ),
            border_style=CFG.jobs_presenter.border_style,
            padding=(1, 1),
            width=panel_width,
            expand=False,
        )

        return Group(Text(""), panel, Text(""))

    def dumpYaml(self) -> None:
        """
        Print the YAML representation of all jobs to stdout.
        """
        for job in self._jobs:
            print(job.toYaml())

    def _createBasicJobsTable(self) -> str:
        """
        Build a compact tabulated string representation of the job list.

        Returns:
            str: Tabulated job information with ANSI color codes applied.

        Notes:
            - Uses `tabulate` with `COMPACT_TABLE` format because
              Rich's Table is prohibitively slow for large number of items.
            - Updates internal job statistics via `self._stats`.
        """
        headers = [
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
        ]

        rows = []
        for job in self._jobs:
            state = job.getState()
            start_time = job.getStartTime() or job.getSubmissionTime()
            end_time = (
                datetime.now()
                if state not in {BatchState.FINISHED, BatchState.FAILED}
                else job.getCompletionTime() or job.getModificationTime()
            )

            cpus = job.getNCPUs()
            gpus = job.getNGPUs()
            nodes = job.getNNodes()
            self._stats.addJob(state, cpus, gpus, nodes)

            row = [
                QQJobsPresenter._color(state.toCode(), state.color),
                QQJobsPresenter._mainColor(QQJobsPresenter._shortenJobId(job.getId())),
                QQJobsPresenter._mainColor(job.getUser()),
                QQJobsPresenter._mainColor(
                    QQJobsPresenter._shortenJobName(job.getName())
                ),
                QQJobsPresenter._mainColor(job.getQueue()),
                QQJobsPresenter._mainColor(str(cpus)),
                QQJobsPresenter._mainColor(str(gpus)),
                QQJobsPresenter._mainColor(str(nodes)),
                QQJobsPresenter._formatTime(
                    state, start_time, end_time, job.getWalltime()
                ),
                QQJobsPresenter._formatNodesOrComment(state, job),
                QQJobsPresenter._formatUtilCPU(job.getUtilCPU()),
                QQJobsPresenter._formatUtilMem(job.getUtilMem()),
                QQJobsPresenter._formatExitCode(job.getExitCode()),
            ]
            rows.append(row)

        return tabulate(
            rows,
            headers=[
                QQJobsPresenter._color(
                    header, color=CFG.jobs_presenter.headers_style, bold=True
                )
                for header in headers
            ],
            tablefmt=QQJobsPresenter.COMPACT_TABLE,
            stralign="center",
            numalign="center",
        )

    @staticmethod
    def _formatTime(
        state: BatchState, start_time: datetime, end_time: datetime, walltime: timedelta
    ) -> str:
        """
        Format the job running time, queued time or completion time with color coding.

        Args:
            state (BatchState): Current job state.
            start_time (datetime): Job submission or start time.
            end_time (datetime): Job completion or current time.
            walltime (timedelta): Scheduled walltime for the job.

        Returns:
            str: ANSI-colored string representing elapsed or finished time.
        """
        match state:
            case BatchState.UNKNOWN | BatchState.SUSPENDED:
                return ""
            case BatchState.FAILED | BatchState.FINISHED:
                return QQJobsPresenter._color(
                    end_time.strftime(CFG.date_formats.standard), color=state.color
                )
            case (
                BatchState.HELD
                | BatchState.QUEUED
                | BatchState.WAITING
                | BatchState.MOVING
            ):
                return QQJobsPresenter._color(
                    format_duration_wdhhmmss(end_time - start_time),
                    color=state.color,
                )
            case BatchState.RUNNING | BatchState.EXITING:
                run_time = end_time - start_time
                return QQJobsPresenter._color(
                    format_duration_wdhhmmss(run_time),
                    color=CFG.jobs_presenter.strong_warning_style
                    if run_time > walltime
                    else state.color,
                ) + QQJobsPresenter._mainColor(
                    f" / {format_duration_wdhhmmss(walltime)}"
                )

        return Text("")

    @staticmethod
    def _formatUtilCPU(util: int | None) -> str:
        """
        Format CPU utilization with color coding.

        Args:
            util (int | None): CPU usage percentage.

        Returns:
            str: ANSI-colored string representation of CPU utilization,
                 or empty string if `util` is None.
        """
        if util is None:
            return ""

        if util > 100:
            color = CFG.jobs_presenter.strong_warning_style
        elif util >= 80:
            color = CFG.jobs_presenter.main_style
        elif util >= 60:
            color = CFG.jobs_presenter.mild_warning_style
        else:
            color = CFG.jobs_presenter.strong_warning_style

        return QQJobsPresenter._color(str(util), color=color)

    @staticmethod
    def _formatUtilMem(util: int | None) -> str:
        """
        Format memory utilization with color coding.

        Args:
            util (int | None): Memory usage percentage.

        Returns:
            str: ANSI-colored string representation of memory utilization,
                 or empty string if `util` is None.
        """
        if util is None:
            return ""

        if util < 90:
            color = CFG.jobs_presenter.main_style
        elif util < 100:
            color = CFG.jobs_presenter.mild_warning_style
        else:
            color = CFG.jobs_presenter.strong_warning_style

        return QQJobsPresenter._color(str(util), color=color)

    @staticmethod
    def _formatExitCode(exit_code: int | None) -> str:
        """
        Format the job exit code with appropriate coloring.

        Args:
            exit_code (int | None): Job exit code.

        Returns:
            str: ANSI-colored exit code. Empty string if None.
        """
        if exit_code is None:
            return ""

        if exit_code == 0:
            return QQJobsPresenter._mainColor(str(exit_code))

        return QQJobsPresenter._color(
            str(exit_code), color=CFG.jobs_presenter.strong_warning_style
        )

    @staticmethod
    def _formatNodesOrComment(state: BatchState, job: BatchJobInterface) -> str:
        """
        Format node information or an estimated runtime comment.

        Args:
            state (BatchState): Current job state.
            job (BatchJobInterface): Job information object.

        Returns:
            str: ANSI-colored string for working node(s) or estimated start,
                 or an empty string if neither information is available.
        """
        if nodes := job.getShortNodes():
            return QQJobsPresenter._mainColor(
                QQJobsPresenter._shortenNodes(" + ".join(nodes)),
            )

        if state in {BatchState.FINISHED, BatchState.FAILED}:
            return ""

        if estimated := job.getEstimated():
            return QQJobsPresenter._color(
                QQJobsPresenter._shortenNodes(
                    f"{estimated[1]} in {format_duration_wdhhmmss(estimated[0] - datetime.now()).rsplit(':', 1)[0]}"
                ),
                color=state.color,
            )

        return ""

    @staticmethod
    def _shortenJobId(job_id: str) -> str:
        """
        Shorten the job ID to its primary component (before the first dot).

        Args:
            job_id (str): Full job identifier.

        Returns:
            str: Shortened job ID.
        """
        return job_id.split(".", 1)[0]

    @staticmethod
    def _shortenJobName(job_name: str) -> str:
        """
        Truncate a job name if it exceeds the maximum allowed display length.

        Args:
            job_name (str): The original job name string.

        Returns:
            str: The possibly shortened job name. If the original name length is
                less than or equal to the configured limit, it is returned unchanged.
        """
        if len(job_name) > CFG.jobs_presenter.max_job_name_length:
            return f"{job_name[: CFG.jobs_presenter.max_job_name_length]}…"

        return job_name

    @staticmethod
    def _shortenNodes(nodes: str) -> str:
        """
        Truncate a list of nodes if it exceeds the maximum allowed display length.

        Args:
            nodes (str): The original nodes string.

        Returns:
            str: The possibly shortened list of nodes. If the original string length
                is less than or equal to the configured limit, it is returned unchanged.
        """
        if len(nodes) > CFG.jobs_presenter.max_nodes_length:
            return f"{nodes[: CFG.jobs_presenter.max_nodes_length]}…"

        return nodes

    @staticmethod
    def _color(string: str, color: str | None = None, bold: bool = False) -> str:
        """
        Apply ANSI color codes and optional bold styling to a string.

        Args:
            string (str): The string to colorize.
            color (str | None): Optional color.
            bold (bool): Whether to apply bold formatting.

        Returns:
            str: ANSI-colored and optionally bolded string.
        """
        return f"{QQJobsPresenter.ANSI_COLORS['bold'] if bold else ''}{QQJobsPresenter.ANSI_COLORS[color] if color else ''}{string}{QQJobsPresenter.ANSI_COLORS['reset'] if color or bold else ''}"

    @staticmethod
    def _mainColor(string: str, bold: bool = False) -> str:
        """
        Apply the main presenter color with optional bold styling.

        Args:
            string (str): String to format.
            bold (bool): Whether to apply bold formatting.

        Returns:
            str: ANSI-colored string in the main presenter color.
        """
        return QQJobsPresenter._color(string, CFG.jobs_presenter.main_style, bold)

    @staticmethod
    def _secondaryColor(string: str, bold: bool = False) -> str:
        """
        Apply the secondary presenter color with optional bold styling.

        Args:
            string (str): String to format.
            bold (bool): Whether to apply bold formatting.

        Returns:
            Text: ANSI-colored Rich Text object in secondary color.
        """
        return QQJobsPresenter._color(string, CFG.jobs_presenter.secondary_style, bold)


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

    # Number of CPUs for unknown jobs.
    n_unknown_cpus: int = 0

    # Number of requested GPUs.
    n_requested_gpus: int = 0

    # Number of allocated GPUs.
    n_allocated_gpus: int = 0

    # Number of GPUs for unknown jobs.
    n_unknown_gpus: int = 0

    # Number of requested nodes.
    n_requested_nodes: int = 0

    # Number of allocated nodes.
    n_allocated_nodes: int = 0

    # Number of nodes for unknown jobs.
    n_unknown_nodes: int = 0

    def addJob(self, state: BatchState, cpus: int, gpus: int, nodes: int) -> None:
        """
        Update the collected resources based on the state of the job.

        Args:
            state (BatchState): State of the job according to the batch system.
            cpus (int): Number of CPUs requested by the job.
            gpus (int): Number of GPUs requested by the job.
            nodes (int): Number of nodes requested by the job.

        Notes:
            - Resources of QUEUED and HELD jobs are counted as REQUESTED.
            - Resources of RUNNING and EXITING jobs are counted as ALLOCATED.
            - Resources of UNKNOWN jobs are counted as UNKNOWN.
            - Resources of jobs in other states are not counted at all.
        """
        try:
            self.n_jobs[state] += 1
        except KeyError:
            self.n_jobs[state] = 1

        if state in {BatchState.QUEUED, BatchState.HELD}:
            self.n_requested_cpus += cpus
            self.n_requested_gpus += gpus
            self.n_requested_nodes += nodes
        elif state in {BatchState.RUNNING, BatchState.EXITING}:
            self.n_allocated_cpus += cpus
            self.n_allocated_gpus += gpus
            self.n_allocated_nodes += nodes
        elif state == BatchState.UNKNOWN:
            self.n_unknown_cpus += cpus
            self.n_unknown_gpus += gpus
            self.n_unknown_nodes += nodes

    def createStatsPanel(self) -> Group:
        """
        Build a Rich Group containing job statistics sections.

        Returns:
            Group: Rich Group with job state counts and resource usage.
        """
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
        """
        Generate Rich Text summarizing the number of jobs in each state.

        Returns:
            Text: Rich Text object listing job states and counts.
        """
        spacing = "    "
        line = Text(spacing)

        line.append(
            QQJobsStatistics._secondaryColorText(f"\n\n Jobs{spacing}", bold=True)
        )

        total = 0
        for state in BatchState:
            if state in self.n_jobs:
                count = self.n_jobs[state]
                total += count
                line.append(
                    QQJobsStatistics._colorText(
                        f"{state.toCode()} ", color=state.color, bold=True
                    )
                )
                line.append(QQJobsStatistics._secondaryColorText(str(count)))
                line.append(spacing)

        # sum of all jobs
        line.append(
            QQJobsStatistics._colorText("Σ ", color=CFG.state_colors.sum, bold=True)
        )
        line.append(QQJobsStatistics._secondaryColorText(str(total)))
        line.append(spacing)

        return line

    def _createResourcesStatsTable(self) -> Table:
        """
        Create a Rich Table summarizing requested and allocated resources.

        Returns:
            Table: Rich Table showing CPU, GPU, and node usage for jobs.
        """
        table = Table(
            show_header=True,
            box=None,
            padding=(0, 1),
        )

        table.add_column("", justify="left")
        table.add_column(QQJobsStatistics._secondaryColorText("CPUs"), justify="center")
        table.add_column(QQJobsStatistics._secondaryColorText("GPUs"), justify="center")
        table.add_column(
            QQJobsStatistics._secondaryColorText("Nodes"), justify="center"
        )

        table.add_row(
            QQJobsStatistics._secondaryColorText("Requested", bold=True),
            QQJobsStatistics._secondaryColorText(str(self.n_requested_cpus)),
            QQJobsStatistics._secondaryColorText(str(self.n_requested_gpus)),
            QQJobsStatistics._secondaryColorText(str(self.n_requested_nodes)),
        )
        table.add_row(
            QQJobsStatistics._secondaryColorText("Allocated", bold=True),
            QQJobsStatistics._secondaryColorText(str(self.n_allocated_cpus)),
            QQJobsStatistics._secondaryColorText(str(self.n_allocated_gpus)),
            QQJobsStatistics._secondaryColorText(str(self.n_allocated_nodes)),
        )
        # unknown resources are displayed only if non-zero
        if (
            self.n_unknown_cpus > 0
            or self.n_unknown_gpus > 0
            or self.n_unknown_nodes > 0
        ):
            table.add_row(
                QQJobsStatistics._secondaryColorText("Unknown", bold=True),
                QQJobsStatistics._secondaryColorText(str(self.n_unknown_cpus)),
                QQJobsStatistics._secondaryColorText(str(self.n_unknown_gpus)),
                QQJobsStatistics._secondaryColorText(str(self.n_unknown_nodes)),
            )

        return table

    @staticmethod
    def _colorText(string: str, color: str | None = None, bold: bool = False) -> Text:
        """
        Create Rich Text with optional color and bold formatting.

        Args:
            string (str): The string to colorize.
            color (str | None): Optional color.
            bold (bool): Whether to apply bold formatting.

        Returns:
            Text: Rich Text object with applied style.
        """
        return Text(string, style=f"{color if color else ''} {'bold' if bold else ''}")

    @staticmethod
    def _secondaryColorText(string: str, bold: bool = False) -> Text:
        """
        Apply the secondary presenter color with optional bold style.

        Args:
            string (str): String to format.
            bold (bool): Whether to apply bold formatting.

        Returns:
            str: Rich Text in main color.
        """
        return QQJobsStatistics._colorText(
            string, color=CFG.jobs_presenter.secondary_style, bold=bold
        )
