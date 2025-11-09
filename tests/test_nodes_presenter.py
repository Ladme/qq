# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import sys
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
import yaml
from rich.console import Group
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from qq_lib.batch.pbs.node import PBSNode
from qq_lib.core.config import CFG
from qq_lib.nodes.presenter import NodeGroup, NodeGroupStats, QQNodesPresenter
from qq_lib.properties.size import Size


@patch.object(NodeGroup, "_shouldShowProperties", return_value=True)
@patch.object(NodeGroup, "_shouldShowSharedScratch", return_value=True)
@patch.object(NodeGroup, "_shouldShowSSDScratch", return_value=True)
@patch.object(NodeGroup, "_shouldShowLocalScratch", return_value=True)
@patch.object(NodeGroup, "_shouldShowGPUMem", return_value=True)
@patch.object(NodeGroup, "_shouldShowGPUs", return_value=True)
@patch.object(NodeGroup, "_setSharedProperties")
@patch.object(NodeGroup, "_sortNodes")
def test_node_group_init(
    mock_sort,
    mock_set_props,
    mock_show_gpus,
    mock_show_gpu_mem,
    mock_show_local,
    mock_show_ssd,
    mock_show_shared,
    mock_show_props,
):
    nodes = [MagicMock(), MagicMock()]
    group = NodeGroup("gpu_nodes", nodes, "user1")

    mock_sort.assert_called_once()
    mock_set_props.assert_called_once()
    mock_show_gpus.assert_called_once()
    mock_show_gpu_mem.assert_called_once()
    mock_show_local.assert_called_once()
    mock_show_ssd.assert_called_once()
    mock_show_shared.assert_called_once()
    mock_show_props.assert_called_once()

    assert group.name == "gpu_nodes"
    assert group.nodes == nodes
    assert isinstance(group.stats, NodeGroupStats)
    assert group._user == "user1"
    assert group._show_gpus is True
    assert group._show_local is True
    assert group._show_ssd is True
    assert group._show_shared is True
    assert group._show_props is True


def test_node_group_should_show_gpus_returns_true_if_any_node_has_gpus():
    node1 = MagicMock(spec=["getNGPUs"])
    node1.getNGPUs.return_value = 0
    node2 = MagicMock(spec=["getNGPUs"])
    node2.getNGPUs.return_value = 4
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    assert group._shouldShowGPUs() is True


def test_node_group_should_show_gpus_returns_false_if_no_node_has_gpus():
    node1 = MagicMock(spec=["getNGPUs"])
    node1.getNGPUs.return_value = 0
    node2 = MagicMock(spec=["getNGPUs"])
    node2.getNGPUs.return_value = 0
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    assert group._shouldShowGPUs() is False


def test_node_group_should_show_gpu_mem_returns_true_if_any_node_has_gpu_mem():
    node1 = MagicMock(spec=["getGPUMemory"])
    node1.getGPUMemory.return_value = Size(0, "kb")
    node2 = MagicMock(spec=["getGPUMemory"])
    node2.getGPUMemory.return_value = Size(4, "gb")
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    assert group._shouldShowGPUMem() is True


def test_node_group_should_show_gpu_mem_returns_false_if_no_node_has_gpu_mem():
    node1 = MagicMock(spec=["getGPUMemory"])
    node1.getGPUMemory.return_value = Size(0, "kb")
    node2 = MagicMock(spec=["getGPUMemory"])
    node2.getGPUMemory.return_value = Size(0, "gb")
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    assert group._shouldShowGPUMem() is False


def test_node_group_should_show_local_scratch_returns_true_if_any_has_space():
    node1 = MagicMock(spec=["getLocalScratch"])
    node1.getLocalScratch.return_value = Size(0, "gb")
    node2 = MagicMock(spec=["getLocalScratch"])
    node2.getLocalScratch.return_value = Size(1024, "gb")
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    assert group._shouldShowLocalScratch() is True


def test_node_group_should_show_local_scratch_returns_false_if_all_zero():
    node1 = MagicMock(spec=["getLocalScratch"])
    node1.getLocalScratch.return_value = Size(0)
    node2 = MagicMock(spec=["getLocalScratch"])
    node2.getLocalScratch.return_value = Size(0)
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    assert group._shouldShowLocalScratch() is False


def test_node_group_should_show_ssd_scratch_returns_true_if_any_has_space():
    node1 = MagicMock(spec=["getSSDScratch"])
    node1.getSSDScratch.return_value = Size(0, "kb")
    node2 = MagicMock(spec=["getSSDScratch"])
    node2.getSSDScratch.return_value = Size(1024, "gb")
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    assert group._shouldShowSSDScratch() is True


def test_node_group_should_show_ssd_scratch_returns_false_if_all_zero():
    node1 = MagicMock(spec=["getSSDScratch"])
    node1.getSSDScratch.return_value = Size(0, "kb")
    node2 = MagicMock(spec=["getSSDScratch"])
    node2.getSSDScratch.return_value = Size(0, "kb")
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    assert group._shouldShowSSDScratch() is False


def test_node_group_should_show_shared_scratch_returns_true_if_any_has_space():
    node1 = MagicMock(spec=["getSharedScratch"])
    node1.getSharedScratch.return_value = Size(0)
    node2 = MagicMock(spec=["getSharedScratch"])
    node2.getSharedScratch.return_value = Size(1024, "gb")
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    assert group._shouldShowSharedScratch() is True


def test_node_group_should_show_shared_scratch_returns_false_if_all_zero():
    node1 = MagicMock(spec=["getSharedScratch"])
    node1.getSharedScratch.return_value = Size(0, "kb")
    node2 = MagicMock(spec=["getSharedScratch"])
    node2.getSharedScratch.return_value = Size(0)
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    assert group._shouldShowSharedScratch() is False


def test_node_group_should_show_properties_returns_true_if_not_all_shared():
    node1 = MagicMock(spec=["getProperties"])
    node1.getProperties.return_value = ["fast", "gpu"]
    node2 = MagicMock(spec=["getProperties"])
    node2.getProperties.return_value = ["fast"]
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    group._shared_properties = {"fast"}
    assert group._shouldShowProperties() is True


def test_node_group_should_show_properties_returns_false_if_all_shared():
    node1 = MagicMock(spec=["getProperties"])
    node1.getProperties.return_value = ["fast", "gpu"]
    node2 = MagicMock(spec=["getProperties"])
    node2.getProperties.return_value = ["fast", "gpu"]
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    group._shared_properties = {"fast", "gpu"}
    assert group._shouldShowProperties() is False


@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._formatNodeProperties",
    return_value=Text("props"),
)
@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._formatSizeProperty",
    return_value=Text("mem"),
)
@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._formatProcessingUnits",
    return_value=Text("cpus"),
)
@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._formatStateMark",
    return_value=Text("state"),
)
def test_node_group_add_node_row_all_shows(
    mock_state_mark, mock_proc_units, mock_size, mock_props
):
    node = MagicMock()
    node.isAvailableToUser.return_value = True
    node.getNFreeCPUs.return_value = 8
    node.getNCPUs.return_value = 16
    node.getNFreeGPUs.return_value = 1
    node.getNGPUs.return_value = 2
    node.getFreeCPUMemory.return_value = "32gb"
    node.getCPUMemory.return_value = "64gb"
    node.getFreeGPUMemory.return_value = "8gb"
    node.getFreeLocalScratch.return_value = "100gb"
    node.getFreeSSDScratch.return_value = "200gb"
    node.getFreeSharedScratch.return_value = "300gb"
    node.getProperties.return_value = ["gpu", "fast"]
    node.getName.return_value = "node1"

    group = NodeGroup.__new__(NodeGroup)
    group._user = "user1"
    group._show_gpus = True
    group._show_gpu_mem = True
    group._show_local = True
    group._show_ssd = True
    group._show_shared = True
    group._show_props = True
    group._shared_properties = set()
    group.stats = MagicMock()

    table = Table()
    table.add_row = MagicMock()  # ty: ignore[invalid-assignment]

    group._addNodeRow(node, table)

    node.isAvailableToUser.assert_called_once_with("user1")
    group.stats.addNode.assert_called_once_with(16, 8, 2, 1, ["gpu", "fast"])
    mock_state_mark.assert_called_once_with(8, 16, 1, 2, True)
    mock_proc_units.assert_any_call(8, 16, True)
    mock_size.assert_any_call("32gb", "64gb", CFG.nodes_presenter.main_text_style)
    mock_props.assert_any_call(
        ["gpu", "fast"], set(), CFG.nodes_presenter.main_text_style
    )

    assert table.add_row.call_count == 1  # ty: ignore[unresolved-attribute]
    assert len(table.add_row.call_args[0]) == 10  # ty: ignore[unresolved-attribute]


@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._formatNodeProperties",
    return_value=Text("props"),
)
@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._formatSizeProperty",
    return_value=Text("mem"),
)
@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._formatProcessingUnits",
    return_value=Text("cpus"),
)
@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._formatStateMark",
    return_value=Text("state"),
)
def test_node_group_add_node_row_all_shows_false(
    mock_state_mark, mock_proc_units, mock_size, mock_props
):
    node = MagicMock()
    node.isAvailableToUser.return_value = False
    node.getNFreeCPUs.return_value = 4
    node.getNCPUs.return_value = 8
    node.getNFreeGPUs.return_value = 0
    node.getNGPUs.return_value = 0
    node.getFreeCPUMemory.return_value = "16gb"
    node.getCPUMemory.return_value = "32gb"
    node.getProperties.return_value = []
    node.getName.return_value = "node2"

    group = NodeGroup.__new__(NodeGroup)
    group._user = "user2"
    group._show_gpus = False
    group._show_gpu_mem = False
    group._show_local = False
    group._show_ssd = False
    group._show_shared = False
    group._show_props = False
    group._shared_properties = set()
    group.stats = MagicMock()

    table = Table()
    table.add_row = MagicMock()  # ty: ignore[invalid-assignment]

    group._addNodeRow(node, table)

    node.isAvailableToUser.assert_called_once_with("user2")
    group.stats.addNode.assert_called_once_with(8, 4, 0, 0, [])
    mock_state_mark.assert_called_once_with(4, 8, 0, 0, False)
    mock_proc_units.assert_any_call(4, 8, False)
    mock_props.assert_not_called()
    mock_size.assert_any_call(
        "16gb", "32gb", CFG.nodes_presenter.unavailable_node_style
    )

    assert table.add_row.call_count == 1  # ty: ignore[unresolved-attribute]
    assert len(table.add_row.call_args[0]) == 4  # ty: ignore[unresolved-attribute]


def test_node_group_set_shared_properties_with_common_values():
    node1 = MagicMock()
    node1.getProperties.return_value = ["gpu", "fast", "linux"]
    node2 = MagicMock()
    node2.getProperties.return_value = ["gpu", "fast", "debian"]
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]

    group._setSharedProperties()

    node1.getProperties.assert_called_once()
    node2.getProperties.assert_called_once()
    assert group._shared_properties == ["fast", "gpu"]


def test_node_group_set_shared_properties_with_no_common_values():
    node1 = MagicMock()
    node1.getProperties.return_value = ["gpu"]
    node2 = MagicMock()
    node2.getProperties.return_value = ["cpu"]
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]

    group._setSharedProperties()

    assert group._shared_properties == []


def test_node_group_set_shared_properties_with_empty_nodes_list():
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = []

    group._setSharedProperties()

    assert not hasattr(group, "_shared_properties")


def test_node_group_sort_nodes_sorts_alphanumerically():
    node_a = MagicMock()
    node_b = MagicMock()
    node_c = MagicMock()
    node_a.getName.return_value = "node10"
    node_b.getName.return_value = "node2"
    node_c.getName.return_value = "node1"

    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node_a, node_b, node_c]

    group._sortNodes()

    sorted_names = [n.getName() for n in group.nodes]
    assert sorted_names == ["node1", "node2", "node10"]


def test_node_group_sort_nodes_handles_mixed_case_names():
    node_a = MagicMock()
    node_b = MagicMock()
    node_a.getName.return_value = "Node2"
    node_b.getName.return_value = "node1"

    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node_a, node_b]

    group._sortNodes()

    sorted_names = [n.getName() for n in group.nodes]
    assert sorted_names == ["node1", "Node2"]


def test_node_group_sort_nodes_handles_dash_and_number_sequences():
    node_a = MagicMock()
    node_b = MagicMock()
    node_c = MagicMock()
    node_a.getName.return_value = "node5-18"
    node_b.getName.return_value = "node5-3"
    node_c.getName.return_value = "node4-20"

    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node_a, node_b, node_c]

    group._sortNodes()

    sorted_names = [n.getName() for n in group.nodes]
    assert sorted_names == ["node4-20", "node5-3", "node5-18"]


@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._formatMetadataTable",
    return_value="mock_table",
)
def test_node_group_create_metadata_table_calls_formatter(mock_format):
    group = NodeGroup.__new__(NodeGroup)
    group._shared_properties = ["gpu", "fast"]
    group.stats = MagicMock()

    result = group.createMetadataTable()

    mock_format.assert_called_once_with(
        ["gpu", "fast"], "Shared properties", group.stats
    )
    assert result == "mock_table"


@patch("qq_lib.nodes.presenter.NodeGroup._addNodeRow")
def test_node_group_create_nodes_table(mock_add_row):
    node1 = MagicMock()
    node2 = MagicMock()

    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node1, node2]
    group._show_gpus = True
    group._show_gpu_mem = True
    group._show_local = True
    group._show_ssd = True
    group._show_shared = True
    group._show_props = True

    table = group.createNodesTable()

    expected_headers = [
        "",
        "Name",
        "NCPUs",
        "CPU Mem",
        "NGPUs",
        "GPU Mem",
        "Scratch Local",
        "Scratch SSD",
        "Scratch Shared",
        "Extra Properties",
    ]

    actual_headers = [col.header.plain for col in table.columns]
    assert actual_headers == expected_headers

    assert mock_add_row.call_count == 2
    mock_add_row.assert_any_call(node1, table)
    mock_add_row.assert_any_call(node2, table)
    assert isinstance(table, Table)


@patch("qq_lib.nodes.presenter.NodeGroup._addNodeRow")
def test_node_group_create_nodes_table_respects_disabled_columns(mock_add_row):
    node = MagicMock()
    group = NodeGroup.__new__(NodeGroup)
    group.nodes = [node]
    group._show_gpus = False
    group._show_gpu_mem = False
    group._show_local = False
    group._show_ssd = False
    group._show_shared = False
    group._show_props = False

    table = group.createNodesTable()

    expected_headers = ["", "Name", "NCPUs", "CPU Mem"]
    actual_headers = [col.header.plain for col in table.columns]
    assert actual_headers == expected_headers

    mock_add_row.assert_called_once_with(node, table)
    assert isinstance(table, Table)


@patch("qq_lib.nodes.presenter.NodeGroup.createNodesTable", return_value="nodes_table")
@patch(
    "qq_lib.nodes.presenter.NodeGroup.createMetadataTable",
    return_value="metadata_table",
)
def test_node_group_create_full_info_panel_returns_group(mock_metadata, mock_nodes):
    group = NodeGroup.__new__(NodeGroup)
    result = group.createFullInfoPanel()

    mock_nodes.assert_called_once_with()
    mock_metadata.assert_called_once_with()

    assert isinstance(result, Group)
    assert list(result.renderables) == ["nodes_table", "", "metadata_table"]


def test_node_group_stats_add_node_updates_totals_and_properties():
    stats = NodeGroupStats()

    stats.addNode(16, 8, 2, 1, ["gpu", "fast"])

    assert stats.n_nodes == 1
    assert stats.n_cpus == 16
    assert stats.n_free_cpus == 8
    assert stats.n_gpus == 2
    assert stats.n_free_gpus == 1
    assert stats.properties == {"gpu", "fast"}


def test_node_group_stats_add_node_accumulates_multiple_calls():
    stats = NodeGroupStats()

    stats.addNode(8, 4, 1, 1, ["gpu"])
    stats.addNode(16, 8, 0, 0, ["fast", "linux"])

    assert stats.n_nodes == 2
    assert stats.n_cpus == 24
    assert stats.n_free_cpus == 12
    assert stats.n_gpus == 1
    assert stats.n_free_gpus == 1
    assert stats.properties == {"gpu", "fast", "linux"}


def test_node_group_stats_add_node_handles_empty_properties():
    stats = NodeGroupStats()

    stats.addNode(4, 2, 0, 0, [])

    assert stats.n_nodes == 1
    assert stats.n_cpus == 4
    assert stats.n_free_cpus == 2
    assert stats.n_gpus == 0
    assert stats.n_free_gpus == 0
    assert stats.properties == set()


def test_node_group_stats_sum_stats_single():
    s1 = NodeGroupStats(
        n_nodes=1,
        n_cpus=16,
        n_free_cpus=8,
        n_gpus=2,
        n_free_gpus=1,
        properties={"gpu"},
    )

    result = NodeGroupStats.sumStats(s1)

    assert result.n_nodes == 1
    assert result.n_cpus == 16
    assert result.n_free_cpus == 8
    assert result.n_gpus == 2
    assert result.n_free_gpus == 1
    assert result.properties == {"gpu"}


def test_node_group_stats_sum_stats_multiple():
    s1 = NodeGroupStats(
        n_nodes=2,
        n_cpus=32,
        n_free_cpus=16,
        n_gpus=4,
        n_free_gpus=2,
        properties={"gpu", "fast"},
    )
    s2 = NodeGroupStats(
        n_nodes=3,
        n_cpus=48,
        n_free_cpus=20,
        n_gpus=6,
        n_free_gpus=3,
        properties={"gpu", "linux"},
    )

    result = NodeGroupStats.sumStats(s1, s2)

    assert isinstance(result, NodeGroupStats)
    assert result.n_nodes == 5
    assert result.n_cpus == 80
    assert result.n_free_cpus == 36
    assert result.n_gpus == 10
    assert result.n_free_gpus == 5
    assert result.properties == {"gpu", "fast", "linux"}


def test_node_group_stats_sum_stats_no():
    result = NodeGroupStats.sumStats()
    assert result.n_nodes == 0
    assert result.n_cpus == 0
    assert result.n_free_cpus == 0
    assert result.n_gpus == 0
    assert result.n_free_gpus == 0
    assert result.properties == set()


@patch("rich.table.Table.add_row")
def test_node_group_stats_create_stats_table_includes_gpu_columns(mock_add_row):
    stats = NodeGroupStats(
        n_nodes=3, n_cpus=48, n_free_cpus=24, n_gpus=6, n_free_gpus=3
    )

    table = stats.createStatsTable()

    headers = [
        col.header.plain if isinstance(col.header, Text) else col.header
        for col in table.columns
    ]
    assert headers == ["", "CPUs", "GPUs", "Nodes"]

    assert mock_add_row.call_count == 2

    total_args, _ = mock_add_row.call_args_list[0]
    assert len(total_args) == 4
    assert str(total_args[0]) == "Total"
    assert str(total_args[1]) == "48"
    assert str(total_args[2]) == "6"
    assert str(total_args[3]) == "3"

    free_args, _ = mock_add_row.call_args_list[1]
    assert len(free_args) == 4
    assert str(free_args[0]) == "Free"
    assert str(free_args[1]) == "24"
    assert str(free_args[2]) == "3"
    assert str(free_args[3]) == ""


@patch("rich.table.Table.add_row")
def test_node_group_stats_create_stats_table_excludes_gpu_columns_when_none(
    mock_add_row,
):
    stats = NodeGroupStats(
        n_nodes=2, n_cpus=32, n_free_cpus=12, n_gpus=0, n_free_gpus=0
    )

    table = stats.createStatsTable()

    headers = [
        col.header.plain if isinstance(col.header, Text) else col.header
        for col in table.columns
    ]
    assert headers == ["", "CPUs", "Nodes"]

    assert mock_add_row.call_count == 2

    total_args, _ = mock_add_row.call_args_list[0]
    assert len(total_args) == 3
    assert str(total_args[0]) == "Total"
    assert str(total_args[1]) == "32"
    assert str(total_args[2]) == "2"

    free_args, _ = mock_add_row.call_args_list[1]
    assert len(free_args) == 3
    assert str(free_args[0]) == "Free"
    assert str(free_args[1]) == "12"
    assert str(free_args[2]) == ""


@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._createNodeGroups",
    return_value=["group1", "group2"],
)
def test_qq_nodes_presenter_init(mock_create_groups):
    nodes = [MagicMock(), MagicMock()]
    presenter = QQNodesPresenter(nodes, "user1", True)

    assert presenter._nodes == nodes
    assert presenter._user == "user1"
    assert presenter._display_all is True
    mock_create_groups.assert_called_once_with()
    assert presenter._node_groups == ["group1", "group2"]


@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._formatPropertiesSection",
    return_value="props_section",
)
def test_qq_nodes_presenter_format_metadata_table_creates_table(mock_props_section):
    stats = MagicMock()
    stats.createStatsTable.return_value = "stats_table"

    result = QQNodesPresenter._formatMetadataTable(["gpu", "fast"], "Shared", stats)

    mock_props_section.assert_called_once_with(["gpu", "fast"], "Shared")
    stats.createStatsTable.assert_called_once_with()

    assert isinstance(result, Table)
    assert len(result.columns) == 2
    assert result.columns[0].max_width == CFG.nodes_presenter.max_props_panel_width


def test_qq_nodes_presenter_format_properties_section_returns_expected_text():
    props = ["gpu", "fast", "linux"]
    title = "Shared properties"

    result = QQNodesPresenter._formatPropertiesSection(props, title)

    assert isinstance(result, Text)
    assert result.plain.startswith("Shared properties:")
    assert "fast, gpu, linux" in result.plain
    # ljust padding
    assert (
        len(result.plain) - len("Shared properties: ")
        == CFG.nodes_presenter.max_props_panel_width
    )


@pytest.mark.parametrize(
    "free_cpus,total_cpus,free_gpus,total_gpus,available,expected_style",
    [
        (0, 8, 0, 1, False, CFG.nodes_presenter.unavailable_node_style),
        (8, 8, 0, 1, False, CFG.nodes_presenter.unavailable_node_style),
        (8, 8, 1, 1, True, CFG.nodes_presenter.free_node_style),
        (4, 8, 0, 1, True, CFG.nodes_presenter.part_free_node_style),
        (0, 8, 0, 1, True, CFG.nodes_presenter.busy_node_style),
    ],
)
def test_qq_nodes_presenter_format_state_mark_returns_correct_style(
    free_cpus, total_cpus, free_gpus, total_gpus, available, expected_style
):
    result = QQNodesPresenter._formatStateMark(
        free_cpus, total_cpus, free_gpus, total_gpus, available
    )

    assert isinstance(result, Text)
    assert result.plain == CFG.nodes_presenter.state_mark
    assert result.style == expected_style


def test_qq_nodes_presenter_format_node_properties():
    props = ["gpu", "fast", "linux"]
    shared = ["linux"]
    style = "bold"

    result = QQNodesPresenter._formatNodeProperties(props, shared, style)

    assert isinstance(result, Text)
    assert result.plain == "gpu, fast"
    assert result.style == "bold"


def test_qq_nodes_presenter_format_node_properties_all_shared():
    props = ["gpu", "fast"]
    shared = ["gpu", "fast"]
    style = "bold"

    result = QQNodesPresenter._formatNodeProperties(props, shared, style)

    assert isinstance(result, Text)
    assert result.plain == ""
    assert result.style == "bold"


def test_qq_nodes_presenter_format_size_property():
    free = Size(8, "gb")
    total = Size(32, "gb")
    style = "bright_green"

    result = QQNodesPresenter._formatSizeProperty(free, total, style)

    assert isinstance(result, Text)
    assert result.plain == "8gb / 32gb"
    assert result.style == "bright_green"


@pytest.mark.parametrize(
    "free,total,available,expected_style",
    [
        (0, 8, False, CFG.nodes_presenter.unavailable_node_style),
        (8, 8, False, CFG.nodes_presenter.unavailable_node_style),
        (0, 0, True, CFG.nodes_presenter.main_text_style),
        (8, 8, True, CFG.nodes_presenter.free_node_style),
        (4, 8, True, CFG.nodes_presenter.part_free_node_style),
        (0, 8, True, CFG.nodes_presenter.busy_node_style),
    ],
)
def test_qq_nodes_presenter_format_processing_units_returns_correct_style(
    free, total, available, expected_style
):
    result = QQNodesPresenter._formatProcessingUnits(free, total, available)

    assert isinstance(result, Text)
    assert result.plain == f"{free} / {total}"
    assert result.style == expected_style


def test_qq_nodes_presenter_interleave():
    sections = [Group("a"), Group("b"), Group("c")]
    seps = [Group("-"), Group("="), Group("*")]

    result = QQNodesPresenter._interleave(sections, seps)

    assert result == [
        sections[0],
        seps[0],
        sections[1],
        seps[1],
        sections[2],
        seps[2],
    ]


def test_qq_nodes_presenter_create_separator_returns_expected_group():
    title = "GPU Nodes"
    result = QQNodesPresenter._createSeparator(title)

    assert isinstance(result, Group)
    assert len(result.renderables) == 3
    assert isinstance(result.renderables[1], Rule)

    rule = result.renderables[1]
    assert isinstance(rule.title, Text)
    assert rule.title.plain == f"NODE GROUP: {title}"
    assert rule.title.style == CFG.nodes_presenter.title_style
    assert rule.style == CFG.nodes_presenter.rule_style


@patch("qq_lib.nodes.presenter.NodeGroup")
def test_qq_nodes_presenter_create_node_groups_creates_group_for_common_prefix(
    mock_nodegroup,
):
    node1 = MagicMock()
    node2 = MagicMock()
    node3 = MagicMock()
    node4 = MagicMock()
    node1.getName.return_value = "node1"
    node2.getName.return_value = "node2"
    node3.getName.return_value = "node3"
    node4.getName.return_value = "misc1"

    presenter = QQNodesPresenter.__new__(QQNodesPresenter)
    presenter._nodes = [node1, node2, node3, node4]
    presenter._user = "user1"

    result = presenter._createNodeGroups()

    assert mock_nodegroup.call_count == 2
    prefixes = [call.args[0] for call in mock_nodegroup.call_args_list]
    assert "node" in prefixes
    assert CFG.nodes_presenter.others_group_name in prefixes
    assert result == [mock_nodegroup.return_value, mock_nodegroup.return_value]


@patch("qq_lib.nodes.presenter.NodeGroup")
def test_qq_nodes_presenter_create_node_groups_assigns_all_to_all_nodes_when_no_prefix(
    mock_nodegroup,
):
    node1 = MagicMock()
    node2 = MagicMock()
    node1.getName.return_value = "x1"
    node2.getName.return_value = "y2"

    presenter = QQNodesPresenter.__new__(QQNodesPresenter)
    presenter._nodes = [node1, node2]
    presenter._user = "user1"

    result = presenter._createNodeGroups()

    mock_nodegroup.assert_called_once_with(
        CFG.nodes_presenter.all_nodes_group_name, [node1, node2], "user1"
    )
    assert result == [mock_nodegroup.return_value]


@patch("qq_lib.nodes.presenter.NodeGroup")
def test_qq_nodes_presenter_create_node_groups_two_groups(mock_nodegroup):
    node1 = MagicMock()
    node2 = MagicMock()
    node3 = MagicMock()
    node4 = MagicMock()
    node5 = MagicMock()
    node6 = MagicMock()

    node1.getName.return_value = "alpha1"
    node2.getName.return_value = "alpha2"
    node3.getName.return_value = "alpha3"
    node4.getName.return_value = "beta1"
    node5.getName.return_value = "beta2"
    node6.getName.return_value = "beta3"

    presenter = QQNodesPresenter.__new__(QQNodesPresenter)
    presenter._nodes = [node1, node2, node3, node4, node5, node6]
    presenter._user = "userA"

    result = presenter._createNodeGroups()

    assert mock_nodegroup.call_count == 2
    called_prefixes = [call.args[0] for call in mock_nodegroup.call_args_list]
    assert "alpha" in called_prefixes
    assert "beta" in called_prefixes
    assert result == [mock_nodegroup.return_value, mock_nodegroup.return_value]


@patch("qq_lib.nodes.presenter.NodeGroup")
def test_qq_nodes_presenter_create_node_groups_merges_small_groups_together(
    mock_nodegroup,
):
    node1 = MagicMock()
    node2 = MagicMock()
    node3 = MagicMock()
    node4 = MagicMock()
    node5 = MagicMock()
    node1.getName.return_value = "alpha1"
    node2.getName.return_value = "alpha2"
    node3.getName.return_value = "alpha3"
    node4.getName.return_value = "beta1"
    node5.getName.return_value = "gamma1"

    presenter = QQNodesPresenter.__new__(QQNodesPresenter)
    presenter._nodes = [node1, node2, node3, node4, node5]
    presenter._user = "userB"

    result = presenter._createNodeGroups()

    assert mock_nodegroup.call_count == 2

    first_call_args = mock_nodegroup.call_args_list[0].args
    second_call_args = mock_nodegroup.call_args_list[1].args

    assert first_call_args[0] == "alpha"
    assert first_call_args[1] == [node1, node2, node3]
    assert first_call_args[2] == "userB"

    assert second_call_args[0] == CFG.nodes_presenter.others_group_name
    assert node4 in second_call_args[1]
    assert node5 in second_call_args[1]
    assert second_call_args[2] == "userB"

    assert result == [mock_nodegroup.return_value, mock_nodegroup.return_value]


@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._formatMetadataTable",
    return_value="formatted_metadata",
)
@patch("qq_lib.nodes.presenter.NodeGroupStats.sumStats")
def test_qq_nodes_presenter_create_metadata_panel(mock_sum_stats, mock_format_table):
    mock_stats = MagicMock()
    mock_stats.properties = {"gpu", "fast"}
    mock_sum_stats.return_value = mock_stats

    group1 = MagicMock()
    group2 = MagicMock()
    group1.stats = MagicMock()
    group2.stats = MagicMock()

    presenter = QQNodesPresenter.__new__(QQNodesPresenter)
    presenter._node_groups = [group1, group2]

    result = presenter._createMetadataPanel()

    mock_sum_stats.assert_called_once_with(group1.stats, group2.stats)
    args, _ = mock_format_table.call_args
    assert sorted(args[0]) == [
        "fast",
        "gpu",
    ]  # need to sort them because they can be provided in any order
    assert args[1] == "All properties"
    assert args[2] == mock_stats

    assert isinstance(result, Group)
    assert isinstance(result.renderables[1], Rule)
    assert result.renderables[1].title.plain == "OVERALL STATISTICS"
    assert result.renderables[1].title.style == CFG.nodes_presenter.title_style
    assert result.renderables[1].style == CFG.nodes_presenter.rule_style
    assert result.renderables[3] == "formatted_metadata"


@patch("qq_lib.nodes.presenter.get_panel_width", return_value=120)
@patch(
    "qq_lib.nodes.presenter.QQNodesPresenter._createMetadataPanel",
    return_value=Group("meta"),
)
def test_qq_nodes_presenter_create_nodes_info_panel_multiple_groups(
    mock_metadata, mock_width
):
    group1 = MagicMock()
    group2 = MagicMock()
    group1.name = "alpha"
    group2.name = "beta"
    group1.createFullInfoPanel.return_value = Group("panel_alpha")
    group2.createFullInfoPanel.return_value = Group("panel_beta")

    presenter = QQNodesPresenter.__new__(QQNodesPresenter)
    presenter._node_groups = [group1, group2]

    result = presenter.createNodesInfoPanel()

    group1.createFullInfoPanel.assert_called_once()
    group2.createFullInfoPanel.assert_called_once()
    mock_metadata.assert_called_once()
    mock_width.assert_called_once()

    assert isinstance(result, Group)
    assert isinstance(result.renderables[1], Panel)
    assert result.renderables[1].title.plain == "NODE GROUP: alpha"
    assert result.renderables[1].title.style == CFG.nodes_presenter.title_style
    assert result.renderables[1].border_style == CFG.nodes_presenter.border_style


@patch("qq_lib.nodes.presenter.get_panel_width", return_value=100)
@patch("qq_lib.nodes.presenter.QQNodesPresenter._createMetadataPanel")
def test_qq_nodes_presenter_create_nodes_info_panel_single_group(
    mock_metadata, mock_width
):
    group = MagicMock()
    group.name = "solo"
    group.createFullInfoPanel.return_value = Group("panel_solo")

    presenter = QQNodesPresenter.__new__(QQNodesPresenter)
    presenter._node_groups = [group]

    result = presenter.createNodesInfoPanel()

    group.createFullInfoPanel.assert_called_once()
    mock_metadata.assert_not_called()
    mock_width.assert_called_once()

    assert isinstance(result, Group)
    assert isinstance(result.renderables[1], Panel)
    assert result.renderables[1].title.plain == "NODE GROUP: solo"
    assert result.renderables[1].title.style == CFG.nodes_presenter.title_style
    assert result.renderables[1].border_style == CFG.nodes_presenter.border_style


def test_qq_nodes_presenter_dump_yaml_roundtrip():
    info_gpu = {
        "Node": "zero21",
        "Mom": "zero21.cluster.local",
        "ntype": "PBS",
        "state": "job-busy",
        "pcpus": "32",
        "resources_available.arch": "linux",
        "resources_available.cluster": "zero",
        "resources_available.cpu_vendor": "amd",
        "resources_available.ngpus": "4",
        "resources_available.mem": "128673mb",
        "resources_available.ncpus": "32",
        "resources_available.queue_list": "q_gpu",
        "resources_assigned.mem": "33554432kb",
        "resources_assigned.ncpus": "32",
        "resources_assigned.ngpus": "4",
        "resv_enable": "True",
        "sharing": "default_shared",
    }

    info_cpu = {
        "Node": "three3",
        "Mom": "three3.cluster.local",
        "ntype": "PBS",
        "state": "free",
        "pcpus": "40",
        "resources_available.arch": "linux",
        "resources_available.cluster": "three",
        "resources_available.cpu_vendor": "intel",
        "resources_available.ngpus": "0",
        "resources_available.mem": "1031962mb",
        "resources_available.ncpus": "40",
        "resources_available.queue_list": "q_cpu",
        "resv_enable": "True",
        "sharing": "default_shared",
    }

    gpu_node = PBSNode.fromDict("zero21", info_gpu)
    cpu_node = PBSNode.fromDict("three3", info_cpu)

    presenter = QQNodesPresenter([gpu_node, cpu_node], user="testuser", all=True)

    captured = StringIO()
    sys.stdout = captured
    try:
        presenter.dumpYaml()
    finally:
        sys.stdout = sys.__stdout__

    yaml_output = captured.getvalue().strip().split("\n\n")
    reloaded_nodes = []

    for doc in yaml_output:
        if not doc.strip():
            continue
        data = yaml.safe_load(doc)
        name = data["Node"]
        reloaded_nodes.append(PBSNode.fromDict(name, data))

    assert len(reloaded_nodes) == 2

    for orig, loaded in zip([gpu_node, cpu_node], reloaded_nodes):
        assert orig.getName() == loaded.getName()
        assert orig._info.keys() == loaded._info.keys()
        for key in orig._info:
            assert orig._info[key] == loaded._info[key]
