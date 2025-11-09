# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime

from rich.align import Align
from rich.console import Console, Group
from rich.padding import Padding
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from qq_lib.core.common import format_duration_wdhhmmss, get_panel_width
from qq_lib.core.config import CFG
from qq_lib.properties.states import RealState

from .informer import Informer


class QQPresenter:
    """
    Presentation layer for qq job information.
    """

    def __init__(self, informer: Informer):
        """
        Initialize the presenter with an Informer.

        Args:
            informer (Informer): The informer object that provides
                access to qq job metadata and runtime details.
        """
        self._informer = informer

    def createJobStatusPanel(self, console: Console | None = None) -> Group:
        """
        Create a standalone status panel for the job.

        Args:
            console (Console | None): Optional Rich console.
                If not provided, a new Console is created.

        Returns:
            Group: A Rich Group containing the status panel.
        """
        console = console or Console()

        panel = Panel(
            self._createJobStatusTable(self._informer.getRealState()),
            title=Text(
                f"JOB: {self._informer.info.job_id}",
                style=CFG.presenter.job_status_panel.title_style,
                justify="center",
            ),
            border_style=CFG.presenter.job_status_panel.border_style,
            padding=(1, 2),
            width=get_panel_width(
                console,
                3,
                CFG.presenter.job_status_panel.min_width,
                CFG.presenter.job_status_panel.max_width,
            ),
        )

        return Group(Text(""), panel, Text(""))

    def createFullInfoPanel(self, console: Console | None = None) -> Group:
        """
        Create a full job information panel.

        Args:
            console (Console | None): Optional Rich console.
                If not provided, a new Console is created.

        Returns:
            Group: A Rich Group containing the full job info panel.
        """

        console = console or Console()

        state = self._informer.getRealState()
        comment, estimated = self._getCommentAndEstimated(state)

        content = Group(
            Padding(self._createBasicInfoTable(), (0, 2)),
            Text(""),
            Rule(
                title=Text(
                    "RESOURCES", style=CFG.presenter.full_info_panel.title_style
                ),
                style=CFG.presenter.full_info_panel.rule_style,
            ),
            Text(""),
            Padding(
                Align.center(self._createResourcesTable(console.size.width)),
                (0, 2),
            ),
            Text(""),
            Rule(
                title=Text("HISTORY", style=CFG.presenter.full_info_panel.title_style),
                style=CFG.presenter.full_info_panel.rule_style,
            ),
            Text(""),
            Padding(
                self._createJobHistoryTable(state, self._informer.info.job_exit_code),
                (0, 2),
            ),
            Text(""),
            Rule(
                title=Text("STATE", style=CFG.presenter.full_info_panel.title_style),
                style=CFG.presenter.full_info_panel.rule_style,
            ),
            Text(""),
            Padding(self._createJobStatusTable(state, comment, estimated), (0, 2)),
        )

        # combine all sections
        full_panel = Panel(
            content,
            title=Text(
                f"JOB: {self._informer.info.job_id}",
                style=CFG.presenter.full_info_panel.title_style,
                justify="center",
            ),
            border_style=CFG.presenter.full_info_panel.border_style,
            # no horizontal padding so Rule reaches borders
            padding=(1, 0),
            width=get_panel_width(
                console,
                3,
                CFG.presenter.full_info_panel.min_width,
                CFG.presenter.full_info_panel.max_width,
            ),
        )

        return Group(Text(""), full_panel, Text(""))

    def getShortInfo(self) -> Text:
        """
        Return a concise, colorized summary of the job's current state.

        Returns:
            Text: A Rich `Text` object containing the job ID followed by the
            current state, colorized according to the `RealState`.
        """
        state = self._informer.getRealState()
        return (
            Text(self._informer.info.job_id)
            + "    "
            + Text(str(state), style=state.color)
        )

    def _createBasicInfoTable(self) -> Table:
        """
        Create a table with basic job information.

        Returns:
            Table: A Rich table with key-value pairs of basic job details.
        """
        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column(justify="right", style=CFG.presenter.key_style)
        table.add_column(
            justify="left", overflow="fold", style=CFG.presenter.value_style
        )

        table.add_row("Job name:", Text(self._informer.info.job_name))

        loop_info = self._informer.info.loop_info
        job_type_str = str(self._informer.info.job_type)

        if loop_info:
            content = f"{job_type_str} [{loop_info.current}/{loop_info.end}]"
        else:
            content = job_type_str

        table.add_row("Job type:", Text(content))
        table.add_row("Submission queue:", Text(self._informer.info.queue))
        table.add_row("Input machine:", Text(self._informer.info.input_machine))
        table.add_row("Input directory:", Text(str(self._informer.info.input_dir)))
        if self._informer.info.main_node and self._informer.info.all_nodes:
            if len(self._informer.info.all_nodes) == 1:
                table.add_row("Working node:", Text(self._informer.info.main_node))
            else:
                table.add_row(
                    "Working nodes:",
                    Text(" + ".join(self._informer.info.all_nodes)),
                )
        if self._informer.info.work_dir:
            table.add_row(
                "Working directory:",
                Text(str(self._informer.info.work_dir)),
            )

        return table

    def _createResourcesTable(self, term_width: int) -> Table:
        """
        Create a table displaying job resource requirements.

        Args:
            term_width (int): Width of the current terminal, used
                to size the spacer column.

        Returns:
            Table: A Rich table summarizing resource allocations.
        """
        resources = self._informer.info.resources
        table = Table(show_header=False, box=None, padding=(0, 1))

        table.add_column(justify="right", style=CFG.presenter.key_style, no_wrap=True)
        table.add_column(
            justify="left",
            style=CFG.presenter.value_style,
            no_wrap=False,
            overflow="fold",
        )
        # spacer column
        table.add_column(justify="center", width=term_width // 30)
        table.add_column(justify="right", style=CFG.presenter.key_style, no_wrap=True)
        table.add_column(
            justify="left",
            style=CFG.presenter.value_style,
            no_wrap=False,
            overflow="fold",
        )

        fields = vars(resources)

        # filter out None values
        items = [
            (k.replace("_", "-").lower(), str(v))
            for k, v in fields.items()
            if v is not None and k != "props"
        ]

        # translate properties
        if resources.props:
            items.extend([(k, str(v)) for k, v in resources.props.items()])

        for i in range(0, len(items), 2):
            row = items[i]
            if i + 1 < len(items):
                row2 = items[i + 1]
                table.add_row(
                    row[0] + ":",
                    Text(row[1]),
                    "",
                    row2[0] + ":",
                    Text(row2[1]),
                )
            else:
                # only one item left
                table.add_row(row[0] + ":", Text(row[1]), "", "", "")

        return table

    def _createJobHistoryTable(self, state: RealState, exit_code: int | None) -> Table:
        """
        Create a table summarizing the job timeline.

        Args:
            state (RealState): State of the job.

        Returns:
            Table: A Rich table showing the chronological job history.
        """
        submitted = self._informer.info.submission_time
        started = self._informer.info.start_time
        completed = self._informer.info.completion_time

        table = Table(show_header=False, box=None, padding=(0, 1))

        table.add_column(justify="right", style=CFG.presenter.key_style)
        table.add_column(
            justify="left", style=CFG.presenter.value_style, overflow="fold"
        )

        table.add_row("Submitted at:", Text(f"{submitted}"))
        # job started
        if started:
            table.add_row(
                "",
                Text(
                    f"was queued for {format_duration_wdhhmmss(started - submitted)}",
                    style=CFG.presenter.notes_style,
                ),
            )
            table.add_row("Started at:", Text(f"{started}"))
        # job is completed (or was killed after start)
        if started and completed:
            table.add_row(
                "",
                Text(
                    f"was running for {format_duration_wdhhmmss(completed - started)}",
                    style=CFG.presenter.notes_style,
                ),
            )
            table.add_row(
                f"{QQPresenter._translateStateToCompletedMsg(state, exit_code).title()} at:",
                Text(f"{completed}"),
            )
        # job is "completed" (likely killed) but never started running
        elif completed:
            table.add_row(
                "",
                Text(
                    f"was queued for {format_duration_wdhhmmss(completed - submitted)}",
                    style=CFG.presenter.notes_style,
                ),
            )
            table.add_row(
                f"{QQPresenter._translateStateToCompletedMsg(state, exit_code).title()} at:",
                Text(f"{completed}"),
            )

        return table

    def _createJobStatusTable(
        self,
        state: RealState,
        comment: str | None = None,
        estimated: tuple[datetime, str] | None = None,
    ) -> Table:
        """
        Create a table summarizing the current job status.

        Args:
            state (RealState): The current real state of the job.

        Returns:
            Table: A Rich table with job state and details.
        """
        (message, details) = self._getStateMessages(
            state,
            self._informer.info.start_time or self._informer.info.submission_time,
            self._informer.info.completion_time or datetime.now(),
        )

        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column(justify="right", style=CFG.presenter.key_style)
        table.add_column(justify="left", style=CFG.presenter.value_style)

        table.add_row("Job state:", Text(message, style=f"{state.color} bold"))
        if details.strip():
            table.add_row("", Text(details))

        if estimated:
            table.add_row(
                "",
                Text(
                    f"Planned start within {format_duration_wdhhmmss(estimated[0] - datetime.now())} on '{estimated[1]}'",
                    style=CFG.presenter.notes_style,
                ),
            )

        # comment is typically only useful if the estimated start time is not defined
        if not estimated and comment:
            table.add_row("", Text(comment, style=CFG.presenter.notes_style))

        return table

    def _getStateMessages(
        self, state: RealState, start_time: datetime, end_time: datetime
    ) -> tuple[str, str]:
        """
        Map a RealState to human-readable messages.

        Args:
            state (RealState): The current job state.
            start_time (datetime): Start time of the relevant state period.
            end_time (datetime): End time of the relevant state period.

        Returns:
            tuple[str, str]: A tuple containing:
                - A short status message (e.g., "Job is running").
                - Additional details, such as elapsed time or error info.
        """
        match state:
            case RealState.QUEUED:
                return (
                    "Job is queued",
                    f"In queue for {format_duration_wdhhmmss(end_time - start_time)}",
                )
            case RealState.HELD:
                return (
                    "Job is held",
                    f"In queue for {format_duration_wdhhmmss(end_time - start_time)}",
                )
            case RealState.SUSPENDED:
                return ("Job is suspended", "")
            case RealState.WAITING:
                return (
                    "Job is waiting",
                    f"In queue for {format_duration_wdhhmmss(end_time - start_time)}",
                )
            case RealState.RUNNING:
                if not (all_nodes := self._informer.info.all_nodes) or not (
                    main_node := self._informer.info.main_node
                ):
                    nodes = "unknown node(s)"
                elif len(all_nodes) == 1:
                    nodes = f"'{main_node}'"
                else:
                    nodes = f"'{main_node}' and {len(all_nodes) - 1} other node{'s' if len(all_nodes) > 2 else ''}"
                return (
                    "Job is running",
                    f"Running for {format_duration_wdhhmmss(end_time - start_time)} on {nodes}",
                )
            case RealState.BOOTING:
                return (
                    "Job is booting",
                    f"Preparing working directory on '{self._informer.getMainNode()}'",
                )
            case RealState.KILLED:
                return (
                    "Job has been killed",
                    f"Killed at {end_time.strftime(CFG.date_formats.standard)}",
                )
            case RealState.FAILED:
                return (
                    "Job has failed",
                    f"Failed at {end_time.strftime(CFG.date_formats.standard)} [exit code: {self._informer.info.job_exit_code}]",
                )
            case RealState.FINISHED:
                return (
                    "Job has finished",
                    f"Completed at {end_time.strftime(CFG.date_formats.standard)}",
                )
            case RealState.EXITING:
                exit_code = self._informer.info.job_exit_code
                if exit_code is None:
                    # no logged exit code -> job was killed
                    msg = "Job is being killed"
                elif exit_code == 0:
                    msg = "Job is finishing successfully"
                else:
                    msg = f"Job is failing [exit code: {exit_code}]"

                return (
                    "Job is exiting",
                    msg,
                )
            case RealState.IN_AN_INCONSISTENT_STATE:
                return (
                    "Job is in an inconsistent state",
                    "The batch system and qq disagree on the status of the job",
                )
            case RealState.UNKNOWN:
                return (
                    "Job is in an unknown state",
                    "Job is in a state that qq does not recognize",
                )

        return (
            "Job is in an unknown state",
            "Job is in a state that qq does not recognize",
        )

    def _getCommentAndEstimated(
        self, state: RealState
    ) -> tuple[str | None, tuple[datetime, str] | None]:
        """
        Retrieve the job comment and estimated start information
        if the job is queued, held, waiting or suspended.

        For jobs in other states, return (None, None).

        Args:
            state (RealState): The current job state.

        Returns:
            tuple[str | None, tuple[datetime, str] | None]:
                A tuple containing:
                - The job comment as a string, or None if unavailable.
                - A tuple with the estimated start time (datetime) and execution node (str),
                    or None if unavailable or not applicable.
        """
        if state in {
            RealState.QUEUED,
            RealState.HELD,
            RealState.WAITING,
            RealState.SUSPENDED,
        }:
            comment = self._informer.getComment()
            estimated = self._informer.getEstimated()
            return comment, estimated

        return None, None

    @staticmethod
    def _translateStateToCompletedMsg(state: RealState, exit_code: None | int) -> str:
        """
        Translates a RealState and optional exit code into a human-readable completion message.

        Returns:
            str: A string representing the final status of the job/process.
        """
        if state == RealState.FINISHED or (
            state == RealState.EXITING and exit_code == 0
        ):
            return "finished"

        if state == RealState.KILLED or (
            state == RealState.EXITING and exit_code is None
        ):
            return "killed"

        if state == RealState.FAILED or (state == RealState.EXITING and exit_code != 0):
            return "failed"

        return "completed"  # default option; should not happen
