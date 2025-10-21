# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from qq_lib.batch.interface.queue import BatchQueueInterface
from qq_lib.core.common import format_duration_wdhhmmss
from qq_lib.core.config import CFG
from qq_lib.properties.states import BatchState


class QQQueuesPresenter:
    """
    Present information about queues of the batch system.
    """

    def __init__(self, queues: list[BatchQueueInterface], user: str, all: bool):
        """
        Initialize the presenter with a list of queues.

        Args:
            queues (list[BatchQueueInterface]): List of queue information objects
                to be presented.
            user (str): Name of the user for which the queues are displayed.
            all (boolean): Display all queues or only those available to the user.
        """
        self._queues = queues
        self._user = user
        self._display_all = all

    def dumpYaml(self) -> None:
        """
        Print the YAML representation of all queues to stdout.
        """
        for queue in self._queues:
            print(queue.toYaml())

    def createQueuesInfoPanel(self, console: Console | None = None) -> Group:
        """
        Create a Rich panel displaying queue information.

        Args:
            console (Console | None): Optional Rich Console instance.
                If None, a new Console will be created.

        Returns:
            Group: Rich Group containing the queues table.
        """
        console = console or Console()
        term_width = console.size.width
        panel_width = term_width
        min_w, max_w = (
            CFG.queues_presenter.min_width,
            CFG.queues_presenter.max_width,
        )

        if min_w is not None:
            panel_width = max(panel_width, min_w)
        if max_w is not None:
            panel_width = min(panel_width, max_w)

        queues_table = self._createQueuesTable()

        panel = Panel(
            queues_table,
            title=Text(
                f"{'ALL' if self._display_all else 'AVAILABLE'} QUEUES",
                style=CFG.queues_presenter.title_style,
                justify="center",
            ),
            border_style=CFG.queues_presenter.border_style,
            padding=(1, 1),
            width=panel_width,
            expand=False,
        )

        return Group(Text(""), panel, Text(""))

    def _createQueuesTable(self) -> Table:
        """
        Construct and return a formatted Rich Table containing queue information.

        Returns:
            Table: A Rich Table object populated with formatted queue data.
        """
        table = Table(
            show_header=True,
            box=None,
            padding=(0, 1),
        )

        table.add_column(justify="left")
        table.add_column(
            header="Name",
            justify="left",
            header_style=CFG.queues_presenter.headers_style,
        )
        table.add_column(
            header="Priority",
            justify="center",
            header_style=CFG.queues_presenter.headers_style,
        )
        table.add_column(
            header=BatchState.RUNNING.toCode(),
            justify="right",
            header_style=CFG.state_colors.running,
        )
        table.add_column(
            header=BatchState.QUEUED.toCode(),
            justify="right",
            header_style=CFG.state_colors.queued,
        )
        table.add_column(
            header="O", justify="right", header_style=CFG.state_colors.other
        )
        table.add_column(header="Σ", justify="right", header_style=CFG.state_colors.sum)
        table.add_column(
            header="Max Walltime",
            justify="right",
            header_style=CFG.queues_presenter.headers_style,
        )
        table.add_column(
            header="Comment",
            justify="center",
            header_style=CFG.queues_presenter.headers_style,
        )

        visited_queues = set()
        for queue in self._queues:
            if queue.fromRouteOnly():
                continue

            QQQueuesPresenter._addQueueRow(queue, table, self._user)
            visited_queues.add(queue)

            # print all reroutings
            if dest_names := queue.getDestinations():
                destinations = [q for q in self._queues if q._name in dest_names]

                for rerouted in destinations:
                    QQQueuesPresenter._addQueueRow(
                        rerouted,
                        table,
                        self._user,
                        from_route=True,
                        # we set the availability to False if the parent queue is not available
                        available=None
                        if queue.isAvailableToUser(self._user)
                        else False,
                    )
                    visited_queues.add(rerouted)

        # print all unbound queues (from route only queues that do not have a parent)
        if self._display_all and (
            unvisited_queues := set(self._queues) - visited_queues
        ):
            table.add_row(
                Text("?", style=f"{CFG.queues_presenter.dangling_mark_style} bold")
            )
            for queue in unvisited_queues:
                QQQueuesPresenter._addQueueRow(
                    queue, table, self._user, from_route=True, dangling=True
                )

        return table

    @staticmethod
    def _addQueueRow(
        queue: BatchQueueInterface,
        table: Table,
        user: str,
        from_route: bool = False,
        dangling: bool = False,
        available: bool | None = None,
    ):
        """
        Add a formatted row representing a single queue to the given table.

        Args:
            queue (BatchQueueInterface): The queue to display.
            table (Table): The Rich Table instance to modify.
            user (str): The username used to determine queue availability.
            from_route (bool, optional): Indicates whether the queue is a rerouted destination.
                Defaults to False.
            dangling (bool, optional): Marks the queue as unbound destination (no parent route). Defaults to False.
            available (bool | None, optional): Manually overrides queue availability. If None,
                availability is determined automatically. Defaults to None.
        """
        mark = (
            CFG.queues_presenter.main_mark
            if not from_route
            else CFG.queues_presenter.rerouted_mark
        )

        available = available or queue.isAvailableToUser(user)

        if available and dangling:
            mark_style = CFG.queues_presenter.dangling_mark_style
        elif available:
            mark_style = CFG.queues_presenter.available_mark_style
        else:
            mark_style = CFG.queues_presenter.unavailable_mark_style

        text_style = (
            CFG.queues_presenter.main_text_style
            if not from_route
            else CFG.queues_presenter.rerouted_text_style
        )

        table.add_row(
            Text(mark, style=mark_style),
            Text(queue.getName(), style=text_style),
            Text(str(queue.getPriority() or ""), style=text_style),
            Text(str(queue.getRunningJobs()), style=CFG.state_colors.running),
            Text(str(queue.getQueuedJobs()), style=CFG.state_colors.queued),
            Text(str(queue.getOtherJobs()), style=CFG.state_colors.other),
            Text(str(queue.getTotalJobs()), style=CFG.state_colors.sum),
            QQQueuesPresenter._formatWalltime(queue, text_style),
            Text(queue.getComment(), style=text_style),
        )

    @staticmethod
    def _formatWalltime(queue: BatchQueueInterface, style: str) -> Text:
        """
        Format the queue's maximum walltime for display.

        Args:
            queue (BatchQueueInterface): The queue whose walltime is being formatted.
            style (str): The Rich text style to apply.

        Returns:
            Text: A styled Rich Text object containing the formatted walltime,
            or an empty string if no walltime is defined.
        """
        if not (walltime := queue.getMaxWalltime()):
            return Text("")

        return Text(format_duration_wdhhmmss(walltime), style=style)
