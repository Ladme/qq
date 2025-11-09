# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import zip_longest

from rich.console import Console, Group
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from qq_lib.batch.interface.node import BatchNodeInterface
from qq_lib.core.common import get_panel_width
from qq_lib.core.config import CFG
from qq_lib.properties.size import Size


class NodeGroup:
    """
    Represents a logical group of compute nodes within a batch or cluster system.
    """

    def __init__(self, name: str, nodes: list[BatchNodeInterface], user: str):
        """
        Initialize a NodeGroup with a name and a list of nodes for the specified user.

        Args:
            name (str): Name identifying this group.
            nodes (list[BatchNodeInterface]): List of nodes in this group.
            user (str): User to check node availability for.
        """
        self.name = name
        self.nodes = nodes
        self.stats = NodeGroupStats()

        self._user = user

        self._sortNodes()
        self._setSharedProperties()

        self._show_gpus = self._shouldShowGPUs()
        self._show_gpu_mem = self._shouldShowGPUMem()
        self._show_local = self._shouldShowLocalScratch()
        self._show_ssd = self._shouldShowSSDScratch()
        self._show_shared = self._shouldShowSharedScratch()
        self._show_props = self._shouldShowProperties()

    def createFullInfoPanel(self) -> Group:
        """
        Create a complete Rich panel summarizing the nodes in this group
        and their shared resources and statistics.

        Returns:
            Group: A Rich `Group` object containing the node table and metadata table.
        """
        nodes = self.createNodesTable()
        metadata = self.createMetadataTable()

        return Group(
            nodes,
            "",
            metadata,
        )

    def createNodesTable(self) -> Table:
        """
        Create a Rich table displaying node-level information.

        Each row represents a node and displays metrics such as CPU/GPU usage,
        memory utilization, scratch storage, and node-specific properties.

        Returns:
            Table: A Rich `Table` instance summarizing all nodes in this group.
        """
        table = Table(
            show_header=True,
            box=None,
            padding=(0, 1),
        )

        headers = {
            "": True,
            "Name": True,
            "NCPUs": True,
            "CPU Mem": True,
            "NGPUs": self._show_gpus,
            "GPU Mem": self._show_gpu_mem,
            "Scratch Local": self._show_local,
            "Scratch SSD": self._show_ssd,
            "Scratch Shared": self._show_shared,
            "Extra Properties": self._show_props,
        }

        for header, show in headers.items():
            if show:
                table.add_column(
                    header=Text(header, justify="center"),
                    justify="center",
                    header_style=CFG.nodes_presenter.headers_style,
                )

        for node in self.nodes:
            self._addNodeRow(node, table)

        return table

    def createMetadataTable(self) -> Table:
        """
        Create a metadata summary table for the group.

        The metadata table includes shared properties and aggregated resource statistics.

        Returns:
            Table: A Rich `Table` summarizing shared properties and resource totals.
        """
        return QQNodesPresenter._formatMetadataTable(
            self._shared_properties, "Shared properties", self.stats
        )

    def _sortNodes(self) -> None:
        """
        Sort nodes by alphanumeric name.

        Nodes are ordered first by their alphabetic prefix (case-insensitive)
        and then numerically by any digits in their names (e.g., `node2` < `node10`).
        """

        def extract_prefix(name: str):
            match = re.match(r"[A-Za-z]+", name)
            return match.group(0).lower() if match else name.lower()

        def extract_number_sequence(name: str):
            # get individual groups of digits in the name
            # this allows properly sorting even names like 'elmo5-18'
            numbers = re.findall(r"\d+", name)
            return [int(n) for n in numbers] if numbers else [float("inf")]

        self.nodes.sort(
            key=lambda node: (
                extract_prefix(node.getName()),
                extract_number_sequence(node.getName()),
            )
        )

    def _setSharedProperties(self) -> None:
        """
        Determine and store properties shared by all nodes in the group.
        """
        if not self.nodes:
            return

        shared = set(self.nodes[0].getProperties())

        for node in self.nodes[1:]:
            shared &= set(node.getProperties())

        self._shared_properties = sorted(shared)

    def _addNodeRow(self, node: BatchNodeInterface, table: Table) -> None:
        """
        Insert a row into the group table representing a single node and update statistics.

        Args:
            node (BatchNodeInterface): Node whose data will be added.
            table (Table): Rich table to which the row will be appended.
        """
        available = node.isAvailableToUser(self._user)
        style = (
            CFG.nodes_presenter.main_text_style
            if available
            else CFG.nodes_presenter.unavailable_node_style
        )

        free_cpus = node.getNFreeCPUs()
        total_cpus = node.getNCPUs()
        free_gpus = node.getNFreeGPUs()
        total_gpus = node.getNGPUs()

        # add the properties of this node to statistics
        self.stats.addNode(
            total_cpus, free_cpus, total_gpus, free_gpus, node.getProperties()
        )

        content = [
            QQNodesPresenter._formatStateMark(
                free_cpus, total_cpus, free_gpus, total_gpus, available
            ),
            Text(node.getName(), style=style),
            QQNodesPresenter._formatProcessingUnits(free_cpus, total_cpus, available),
            QQNodesPresenter._formatSizeProperty(
                node.getFreeCPUMemory(), node.getCPUMemory(), style
            ),
            QQNodesPresenter._formatProcessingUnits(free_gpus, total_gpus, available)
            if self._show_gpus
            else None,
            Text(str(node.getFreeGPUMemory()), style=style)
            if self._show_gpu_mem
            else None,
            Text(str(node.getFreeLocalScratch()), style=style)
            if self._show_local
            else None,
            Text(str(node.getFreeSSDScratch()), style=style)
            if self._show_ssd
            else None,
            Text(str(node.getFreeSharedScratch()), style=style)
            if self._show_shared
            else None,
            QQNodesPresenter._formatNodeProperties(
                node.getProperties(), self._shared_properties, style
            )
            if self._show_props
            else None,
        ]

        table.add_row(*(x for x in content if x is not None))

    def _shouldShowGPUs(self) -> bool:
        """
        Determine whether the GPUs column should be displayed.

        Returns:
            bool: True if any node has GPUs, False otherwise.
        """
        return any(n.getNGPUs() != 0 for n in self.nodes)

    def _shouldShowGPUMem(self) -> bool:
        """
        Determine whether GPU memory column should be displayed.

        Returns:
            bool: True if any node has GPU memory, False otherwise.
        """
        return any(n.getGPUMemory().value != 0 for n in self.nodes)

    def _shouldShowLocalScratch(self) -> bool:
        """
        Determine whether local scratch storage columns should be displayed.

        Returns:
            bool: True if any node has local scratch space, False otherwise.
        """
        return any(n.getLocalScratch().value != 0 for n in self.nodes)

    def _shouldShowSSDScratch(self) -> bool:
        """
        Determine whether SSD scratch storage columns should be displayed.

        Returns:
            bool: True if any node has SSD scratch space, False otherwise.
        """
        return any(n.getSSDScratch().value != 0 for n in self.nodes)

    def _shouldShowSharedScratch(self) -> bool:
        """
        Determine whether shared scratch storage columns should be displayed.

        Returns:
            bool: True if any node has shared scratch space, False otherwise.
        """
        return any(n.getSharedScratch().value != 0 for n in self.nodes)

    def _shouldShowProperties(self) -> bool:
        """
        Determine whether extra property column should be displayed.

        Returns:
            bool: True if any node has properties not shared by all nodes in the group.
        """
        return any(
            any(p not in self._shared_properties for p in n.getProperties())
            for n in self.nodes
        )


@dataclass
class NodeGroupStats:
    """
    Collects and aggregates statistics for a group of compute nodes.
    """

    # number of nodes in the group
    n_nodes: int = 0

    # number of CPUs in the group
    n_cpus: int = 0

    # number of free CPUs in the group
    n_free_cpus: int = 0

    # number of GPUs in the group
    n_gpus: int = 0

    # number of free GPUs in the group
    n_free_gpus: int = 0

    # all properties in the group
    properties: set[str] = field(default_factory=set)

    def addNode(
        self, cpus: int, free_cpus: int, gpus: int, free_gpus: int, props: list[str]
    ) -> None:
        """
        Add a single node's statistics to the group totals.

        Args:
            cpus (int): Total number of CPUs on the node.
            free_cpus (int): Number of currently available CPUs.
            gpus (int): Total number of GPUs on the node.
            free_gpus (int): Number of currently available GPUs.
            props (list[str]): List of node properties.
        """
        self.n_nodes += 1
        self.n_cpus += cpus
        self.n_free_cpus += free_cpus
        self.n_gpus += gpus
        self.n_free_gpus += free_gpus
        self.properties.update(props)

    def createStatsTable(self) -> Table:
        """
        Create a Rich table summarizing aggregated node statistics.

        Returns:
            Table: A Rich `Table` object summarizing the group's statistics.
        """
        table = Table(
            show_header=True,
            box=None,
            padding=(0, 1),
        )

        style = CFG.nodes_presenter.secondary_text_style

        table.add_column("", justify="left")
        table.add_column(Text("CPUs", style=style), justify="center")
        if self.n_gpus > 0:
            table.add_column(Text("GPUs", style=style), justify="center")
        table.add_column(Text("Nodes", style=style), justify="center")

        content_total = [
            Text("Total", style=f"{style} bold"),
            Text(str(self.n_cpus), style=style),
            Text(str(self.n_gpus), style=style) if self.n_gpus > 0 else None,
            Text(str(self.n_nodes), style=style),
        ]

        table.add_row(*(x for x in content_total if x is not None))

        content_free = [
            Text("Free", style=f"{style} bold"),
            Text(str(self.n_free_cpus), style=style),
            Text(str(self.n_free_gpus), style=style) if self.n_gpus > 0 else None,
            "",
        ]

        table.add_row(*(x for x in content_free if x is not None))

        return table

    @staticmethod
    def sumStats(*stats: "NodeGroupStats") -> "NodeGroupStats":
        """
        Combine multiple `NodeGroupStats` objects into a single aggregate.

        Args:
            *stats (NodeGroupStats): One or more stats objects to sum.

        Returns:
            NodeGroupStats: A new object containing the combined totals and
            union of all properties.
        """
        return NodeGroupStats(
            n_nodes=sum(s.n_nodes for s in stats),
            n_cpus=sum(s.n_cpus for s in stats),
            n_free_cpus=sum(s.n_free_cpus for s in stats),
            n_gpus=sum(s.n_gpus for s in stats),
            n_free_gpus=sum(s.n_free_gpus for s in stats),
            properties=set().union(*(s.properties for s in stats)),
        )


class QQNodesPresenter:
    """
    Presenter class for displaying information about batch system nodes.
    """

    def __init__(self, nodes: list[BatchNodeInterface], user: str, all: bool):
        """
        Initialize the presenter with a list of nodes.

        Args:
            nodes (list[BatchNodeInterface]): List of node information objects
                to be presented.
            user (str): Name of the user for which the nodes are displayed.
            all (boolean): Display all nodes or only those that are available.
        """
        self._nodes = nodes
        self._user = user
        self._display_all = all

        self._node_groups = self._createNodeGroups()

    def dumpYaml(self) -> None:
        """
        Print the YAML representation of all nodes to stdout.
        """
        for node in self._nodes:
            print(node.toYaml())

    def createNodesInfoPanel(self, console: Console | None = None) -> Group:
        """
        Build a complete Rich panel summarizing all node groups.

        Args:
            console (Console | None): Optional Rich console instance used to
                determine available terminal width. If None, a new console
                is created.

        Returns:
            Group: A Rich `Group` containing the formatted node information panel.
        """
        console = console or Console()

        groups = self._node_groups
        parts = [g.createFullInfoPanel() for g in self._node_groups]
        seps = [
            QQNodesPresenter._createSeparator(g.name) for g in self._node_groups[1:]
        ]

        content: list[Group] = QQNodesPresenter._interleave(parts, seps)

        if len(groups) > 1:
            content.append(self._createMetadataPanel())

        panel = Panel(
            Group(*content),
            title=Text(
                f"NODE GROUP: {groups[0].name}",
                style=CFG.nodes_presenter.title_style,
                justify="center",
            ),
            border_style=CFG.nodes_presenter.border_style,
            padding=(1, 1),
            width=get_panel_width(
                console, 1, CFG.nodes_presenter.min_width, CFG.nodes_presenter.max_width
            ),
            expand=False,
        )

        return Group(Text(""), panel, Text(""))

    def _createMetadataPanel(self) -> Group:
        """
        Create a summary panel displaying overall statistics across all node groups.

        Returns:
            Group: A Rich `Group` containing the overall statistics summary.
        """
        total_stats = NodeGroupStats.sumStats(*(g.stats for g in self._node_groups))

        return Group(
            "",
            Rule(
                title=Text(
                    "OVERALL STATISTICS",
                    style=CFG.nodes_presenter.title_style,
                ),
                style=CFG.nodes_presenter.rule_style,
            ),
            "",
            QQNodesPresenter._formatMetadataTable(
                list(total_stats.properties), "All properties", total_stats
            ),
        )

    def _createNodeGroups(self) -> list[NodeGroup]:
        """
        Organize nodes into logical groups based on common name prefixes.

        Nodes sharing the same alphabetic prefix are grouped together (e.g.,
        `node1`, `node2`, `node3` form one group). Groups with fewer than three
        nodes are merged into a generic "Others" group.

        Returns:
            list[NodeGroup]: A list of node groups created from the input nodes.
        """
        raw_groups = defaultdict(list)
        unassigned = []

        # get nodes with same names
        for node in self._nodes:
            name = node.getName()
            match = re.match(r"[A-Za-z]+", name)
            prefix = match.group(0) if match else ""
            raw_groups[prefix].append(node)

        # create node groups; each node group must have at least 3 members
        groups: list[NodeGroup] = []
        for prefix, nodes in raw_groups.items():
            if len(nodes) >= 3:
                groups.append(NodeGroup(prefix, nodes, self._user))
            else:
                unassigned.extend(nodes)

        # create a node group for the unassigned nodes
        if unassigned:
            groups.append(
                NodeGroup(
                    CFG.nodes_presenter.others_group_name
                    if len(groups) > 0
                    else CFG.nodes_presenter.all_nodes_group_name,
                    unassigned,
                    self._user,
                )
            )

        return groups

    def _createSeparator(title: str) -> Group:
        """
        Create a visual separator between node group panels.

        Args:
            title (str): The title to display within the rule separator.

        Returns:
            Group: A Rich `Group` containing a rule with the given title.
        """
        return Group(
            "",
            Rule(
                title=Text(
                    f"NODE GROUP: {title}", style=CFG.nodes_presenter.title_style
                ),
                style=CFG.nodes_presenter.rule_style,
            ),
            "",
        )

    def _interleave(sections: list[Group], seps: list[Group]) -> list[Group]:
        """
        Interleave content sections with separator elements.

        Args:
            sections (list[Group]): Panels or sections to display.
            seps (list[Group]): Separators to insert between sections.

        Returns:
            list[Group]: Combined ordered list of sections and separators.
        """
        chunks = []
        for part, sep in zip_longest(sections, seps, fillvalue=None):
            if part is not None:
                chunks.append(part)
            if sep is not None:
                chunks.append(sep)
        return chunks

    @staticmethod
    def _formatProcessingUnits(free: int, total: int, available: bool) -> Text:
        """
        Format numbers of free and total CPUs or GPUs as a styled Rich text element.

        Args:
            free (int): Number of free units (CPUs or GPUs).
            total (int): Total number of units.
            available (bool): Whether the node is available to the user.

        Returns:
            Text: A styled Rich text element showing free and total counts.
        """
        if not available:
            style = CFG.nodes_presenter.unavailable_node_style
        elif total == 0:
            style = CFG.nodes_presenter.main_text_style
        elif total == free:
            style = CFG.nodes_presenter.free_node_style
        elif free > 0:
            style = CFG.nodes_presenter.part_free_node_style
        else:
            style = CFG.nodes_presenter.busy_node_style

        return Text(f"{free} / {total}", style=style)

    @staticmethod
    def _formatSizeProperty(free: Size, total: Size, style: str) -> Text:
        """
        Format a memory or storage property as a styled text string.

        Args:
            free (Size): Available memory or storage.
            total (Size): Total memory or storage.
            style (str): Rich style string to apply to the text.

        Returns:
            Text: A formatted text element showing free and total capacity.
        """
        return Text(f"{free} / {total}", style=style)

    @staticmethod
    def _formatNodeProperties(
        props: list[str], shared_props: list[str], style: str
    ) -> Text:
        """
        Format node-specific properties for display, excluding shared ones.

        Args:
            props (list[str]): All properties of the node.
            shared_props (list[str]): Properties common to all nodes in the group.
            style (str): Rich text style for formatting.

        Returns:
            Text: Comma-separated list of unique node properties.
        """
        return Text(", ".join(x for x in props if x not in shared_props), style=style)

    @staticmethod
    def _formatStateMark(
        free_cpus: int,
        total_cpus: int,
        free_gpus: int,
        total_gpus: int,
        available: bool,
    ) -> Text:
        """
        Generate a state mark symbol indicating node utilization and availability.

        Args:
            free_cpus (int): Number of free CPU cores.
            total_cpus (int): Total number of CPU cores.
            free_gpus (int): Number of free GPUs.
            total_gpus (int): Total number of GPUs.
            available (bool): Whether the node is accessible to the user.

        Returns:
            Text: A styled Rich text symbol representing node state.
        """
        if not available:
            style = CFG.nodes_presenter.unavailable_node_style
        elif free_cpus == total_cpus and free_gpus == total_gpus:
            style = CFG.nodes_presenter.free_node_style
        elif free_cpus != 0 or free_gpus != 0:
            style = CFG.nodes_presenter.part_free_node_style
        else:
            style = CFG.nodes_presenter.busy_node_style

        return Text(CFG.nodes_presenter.state_mark, style=style)

    @staticmethod
    def _formatPropertiesSection(props: list[str], title: str) -> Text:
        """
        Create a formatted text section showing a list of properties.

        Args:
            props (list[str]): Properties to display.
            title (str): Section label to prefix before the property list.

        Returns:
            Text: A Rich text object combining the title and formatted property list.
        """
        return Text(
            f"{title}: ",
            style=f"{CFG.nodes_presenter.main_text_style} bold",
        ) + Text(
            (", ".join(sorted(props))).ljust(CFG.nodes_presenter.max_props_panel_width),
            style=f"{CFG.nodes_presenter.main_text_style} not bold",
        )

    @staticmethod
    def _formatMetadataTable(
        props: list[str], title: str, stats: NodeGroupStats
    ) -> Table:
        """
        Create a metadata table showing a list of (shared/all) properties
        and an aggregated statistics summary (CPU, GPU, node counts).

        Args:
            props (list[str]): List of properties to display.
            title (str): Title label for the property section.
            stats (NodeGroupStats): Aggregated statistics for the corresponding node group.

        Returns:
            Table: A Rich grid table with property and statistics columns.
        """
        grid = Table.grid(expand=False, padding=(0, 5))
        grid.add_column(max_width=CFG.nodes_presenter.max_props_panel_width)
        grid.add_column()

        grid.add_row(
            QQNodesPresenter._formatPropertiesSection(props, title),
            stats.createStatsTable(),
        )

        return grid
