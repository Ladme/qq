# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import sys
from datetime import timedelta
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
import yaml
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from qq_lib.batch.pbs.queue import PBSQueue
from qq_lib.core.config import CFG
from qq_lib.queues.presenter import QueuesPresenter


def test_queues_presenter_init_sets_fields_correctly():
    queues = [MagicMock(), MagicMock()]
    user = "user"
    display_all = True

    presenter = QueuesPresenter(queues, user, display_all)

    assert presenter._queues == queues
    assert presenter._user == user
    assert presenter._display_all is True


def test_queues_presenter_format_walltime_returns_formatted_text():
    queue = MagicMock()
    queue.getMaxWalltime.return_value = timedelta(days=1, hours=2, minutes=3, seconds=4)

    result = QueuesPresenter._formatWalltime(queue, "cyan")

    assert isinstance(result, Text)
    assert result.plain == "1d 02:03:04"
    assert result.style == "cyan"
    queue.getMaxWalltime.assert_called_once()


def test_queues_presenter_format_walltime_returns_empty_text_when_no_walltime():
    queue = MagicMock()
    queue.getMaxWalltime.return_value = None

    result = QueuesPresenter._formatWalltime(queue, "cyan")

    assert isinstance(result, Text)
    assert result.plain == ""
    queue.getMaxWalltime.assert_called_once()


def test_queues_presenter_add_queue_row_main_available():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = True
    queue.getName.return_value = "mainq"
    queue.getPriority.return_value = "47"
    queue.getRunningJobs.return_value = 5
    queue.getQueuedJobs.return_value = 3
    queue.getOtherJobs.return_value = 2
    queue.getTotalJobs.return_value = 10
    queue.getComment.return_value = "Main queue"
    queue.getMaxWalltime.return_value = None
    queue.getMaxNNodes.return_value = 8

    presenter = QueuesPresenter([queue], "user", True)
    table = Table()
    presenter._addQueueRow(queue, table, user="user")

    buffer = StringIO()
    console = Console(file=buffer, width=120)
    console.print(table)
    output = buffer.getvalue()

    assert "mainq" in output
    assert "47" in output
    assert "5" in output
    assert "3" in output
    assert "2" in output
    assert "10" in output
    assert "8" in output
    assert "Main queue" in output
    queue.isAvailableToUser.assert_called_once_with("user")


def test_queues_presenter_add_queue_row_main_available_do_not_show_comment():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = True
    queue.getName.return_value = "mainq"
    queue.getPriority.return_value = "47"
    queue.getRunningJobs.return_value = 5
    queue.getQueuedJobs.return_value = 3
    queue.getOtherJobs.return_value = 2
    queue.getTotalJobs.return_value = 10
    queue.getComment.return_value = "Main queue"
    queue.getMaxWalltime.return_value = None
    queue.getMaxNNodes.return_value = 8

    presenter = QueuesPresenter([queue], "user", False)
    presenter._show_comment = False
    table = Table()
    presenter._addQueueRow(queue, table, user="user")

    buffer = StringIO()
    console = Console(file=buffer, width=120)
    console.print(table)
    output = buffer.getvalue()

    assert "mainq" in output
    assert "47" in output
    assert "5" in output
    assert "3" in output
    assert "2" in output
    assert "10" in output
    assert "8" in output
    assert "Main queue" not in output
    queue.isAvailableToUser.assert_called_once_with("user")


def test_queues_presenter_add_queue_row_main_available_do_not_show_max_nodes():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = True
    queue.getName.return_value = "mainq"
    queue.getPriority.return_value = "47"
    queue.getRunningJobs.return_value = 5
    queue.getQueuedJobs.return_value = 3
    queue.getOtherJobs.return_value = 2
    queue.getTotalJobs.return_value = 10
    queue.getComment.return_value = "Main queue"
    queue.getMaxWalltime.return_value = None
    queue.getMaxNNodes.return_value = 8

    presenter = QueuesPresenter([queue], "user", True)
    presenter._show_max_nnodes = False
    table = Table()
    presenter._addQueueRow(queue, table, user="user")

    buffer = StringIO()
    console = Console(file=buffer, width=120)
    console.print(table)
    output = buffer.getvalue()

    assert "mainq" in output
    assert "47" in output
    assert "5" in output
    assert "3" in output
    assert "2" in output
    assert "10" in output
    assert "8" not in output
    assert "Main queue" in output
    queue.isAvailableToUser.assert_called_once_with("user")


def test_queues_presenter_add_queue_row_main_unavailable():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = False
    queue.getName.return_value = "main_unavail"
    queue.getPriority.return_value = "0"
    queue.getRunningJobs.return_value = 0
    queue.getQueuedJobs.return_value = 1
    queue.getOtherJobs.return_value = 0
    queue.getTotalJobs.return_value = 1
    queue.getComment.return_value = "No access"
    queue.getMaxWalltime.return_value = None
    queue.getMaxNNodes.return_value = 8

    presenter = QueuesPresenter([queue], "user", True)
    table = Table()
    presenter._addQueueRow(queue, table, user="user")

    buffer = StringIO()
    console = Console(file=buffer, width=160, force_terminal=False, color_system=None)
    console.print(table)
    output = buffer.getvalue()

    assert "main_unavail" in output
    assert "0" in output
    assert "1" in output
    assert "8" in output
    assert "No access" in output
    queue.isAvailableToUser.assert_called_once_with("user")


def test_queues_presenter_add_queue_row_rerouted_available():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = True
    queue.getName.return_value = "reroutedq"
    queue.getPriority.return_value = "7"
    queue.getRunningJobs.return_value = 2
    queue.getQueuedJobs.return_value = 4
    queue.getOtherJobs.return_value = 1
    queue.getTotalJobs.return_value = 7
    queue.getComment.return_value = "Rerouted ok"
    queue.getMaxWalltime.return_value = None
    queue.getMaxNNodes.return_value = 8

    presenter = QueuesPresenter([queue], "user", False)
    table = Table()
    presenter._addQueueRow(queue, table, user="user", from_route=True)

    buffer = StringIO()
    console = Console(file=buffer, width=160, force_terminal=False, color_system=None)
    console.print(table)
    output = buffer.getvalue()

    assert "reroutedq" in output
    assert "7" in output
    assert "2" in output
    assert "4" in output
    assert "1" in output
    assert "8" in output
    assert "Rerouted ok" in output
    assert CFG.queues_presenter.rerouted_mark in output
    queue.isAvailableToUser.assert_called_once_with("user")


def test_queues_presenter_add_queue_row_rerouted_unavailable():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = False
    queue.getName.return_value = "rerouted_blocked"
    queue.getPriority.return_value = "3"
    queue.getRunningJobs.return_value = 0
    queue.getQueuedJobs.return_value = 0
    queue.getOtherJobs.return_value = 0
    queue.getTotalJobs.return_value = 0
    queue.getComment.return_value = "Rerouted blocked"
    queue.getMaxWalltime.return_value = None

    presenter = QueuesPresenter([queue], "user", True)
    table = Table()
    presenter._addQueueRow(queue, table, user="user", from_route=True)

    buffer = StringIO()
    console = Console(file=buffer, width=160, force_terminal=False, color_system=None)
    console.print(table)
    output = buffer.getvalue()

    assert "rerouted_blocked" in output
    assert "3" in output
    assert "Rerouted blocked" in output
    assert CFG.queues_presenter.rerouted_mark in output
    queue.isAvailableToUser.assert_called_once_with("user")


def test_queues_presenter_add_queue_row_dangling():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = True
    queue.getName.return_value = "danglingq"
    queue.getPriority.return_value = "T7 (3)"
    queue.getRunningJobs.return_value = 1
    queue.getQueuedJobs.return_value = 1
    queue.getOtherJobs.return_value = 1
    queue.getTotalJobs.return_value = 3
    queue.getComment.return_value = "Dangling dest"
    queue.getMaxWalltime.return_value = None

    presenter = QueuesPresenter([queue], "user", False)
    table = Table()
    presenter._addQueueRow(queue, table, user="user", dangling=True)

    buffer = StringIO()
    console = Console(file=buffer, width=160, force_terminal=False, color_system=None)
    console.print(table)
    output = buffer.getvalue()

    assert "danglingq" in output
    assert "T7 (3)" in output
    assert "1" in output
    assert "3" in output
    assert "Dangling dest" in output
    queue.isAvailableToUser.assert_called_once_with("user")


def _make_queue(
    name: str,
    *,
    from_route_only: bool = False,
    destinations: list[str] | None = None,
    available_to: bool = True,
    priority: str | None = "10",
    running: int = 1,
    queued: int = 2,
    other: int = 3,
    total: int = 6,
    comment: str = "comment",
    walltime: object = None,
    nnodes: int | None = 2,
):
    q = MagicMock()
    q._name = name
    q.fromRouteOnly.return_value = from_route_only
    q.getDestinations.return_value = destinations or []
    q.isAvailableToUser.return_value = available_to
    q.getName.return_value = name
    q.getPriority.return_value = priority
    q.getRunningJobs.return_value = running
    q.getQueuedJobs.return_value = queued
    q.getOtherJobs.return_value = other
    q.getTotalJobs.return_value = total
    q.getComment.return_value = comment
    q.getMaxWalltime.return_value = walltime
    q.getMaxNNodes.return_value = nnodes
    return q


def _render_table(table: Table) -> str:
    buf = StringIO()
    Console(file=buf, width=200, force_terminal=False, color_system=None).print(table)
    return buf.getvalue()


def test_queues_presenter_create_queues_table_basic_main_only():
    main = _make_queue("mainq", destinations=[])
    presenter = QueuesPresenter([main], user="user", all=False)

    table = presenter._createQueuesTable()
    output = _render_table(table)

    assert "Name" in output
    assert "Priority" in output
    assert "Comment" in output
    assert "Max Nodes" in output
    assert CFG.queues_presenter.main_mark in output
    assert "mainq" in output
    main.isAvailableToUser.assert_called_once_with("user")


def test_queues_presenter_create_queues_table_no_comment():
    main = _make_queue("mainq", destinations=[])
    presenter = QueuesPresenter([main], user="user", all=False)
    presenter._show_comment = False

    table = presenter._createQueuesTable()
    output = _render_table(table)

    assert "Name" in output
    assert "Priority" in output
    assert "Comment" not in output
    assert CFG.queues_presenter.main_mark in output
    assert "mainq" in output
    main.isAvailableToUser.assert_called_once_with("user")


def test_queues_presenter_create_queues_table_basic_no_max_nodes():
    main = _make_queue("mainq", destinations=[], nnodes=None)
    presenter = QueuesPresenter([main], user="user", all=False)

    table = presenter._createQueuesTable()
    output = _render_table(table)

    assert "Name" in output
    assert "Priority" in output
    assert "Comment" in output
    assert "Max Nodes" not in output
    assert CFG.queues_presenter.main_mark in output
    assert "mainq" in output
    main.isAvailableToUser.assert_called_once_with("user")


def test_queues_presenter_create_queues_table_with_rerouted_parent_available():
    main = _make_queue(
        "mainq", destinations=["destq"], available_to=True, comment="main"
    )
    dest = _make_queue("destq", comment="dest")
    presenter = QueuesPresenter([main, dest], user="user", all=False)

    table = presenter._createQueuesTable()
    output = _render_table(table)

    assert CFG.queues_presenter.main_mark in output
    assert CFG.queues_presenter.rerouted_mark in output
    assert "mainq" in output
    assert "destq" in output


def test_queues_presenter_create_queues_table_with_rerouted_parent_unavailable():
    main = _make_queue("mainq", destinations=["destq"], available_to=False)
    dest = _make_queue("destq")
    presenter = QueuesPresenter([main, dest], user="user", all=False)

    table = presenter._createQueuesTable()
    output = _render_table(table)

    assert "mainq" in output
    assert "destq" in output
    assert CFG.queues_presenter.main_mark in output
    assert CFG.queues_presenter.rerouted_mark in output


def test_queues_presenter_create_queues_table_unbound_when_all_true():
    route_only_unbound = _make_queue(
        "lonely_dest", from_route_only=True, comment="dangling"
    )
    presenter = QueuesPresenter([route_only_unbound], user="user", all=True)

    table = presenter._createQueuesTable()
    output = _render_table(table)

    # dangling mark row indicator and the unbound queue should be printed
    assert "?" in output
    assert "lonely_dest" in output
    route_only_unbound.isAvailableToUser.assert_called_once_with("user")


@pytest.mark.parametrize("all", [False, True])
def test_queues_presenter_create_queues_info_panel_structure(all):
    queue_mock = MagicMock()
    presenter = QueuesPresenter([queue_mock], user="user", all=all)

    fake_table = Table()
    with patch.object(presenter, "_createQueuesTable", return_value=fake_table):
        panel_group = presenter.createQueuesInfoPanel()

    # structure of returned object
    assert isinstance(panel_group, Group)
    assert len(panel_group.renderables) == 3

    # middle renderable must be a Panel
    main_panel = panel_group.renderables[1]
    assert isinstance(main_panel, Panel)

    # title
    assert isinstance(main_panel.title, Text)
    if all:
        assert "ALL QUEUES" in main_panel.title.plain
    else:
        assert "AVAILABLE QUEUES" in main_panel.title.plain

    # content should be a table
    assert isinstance(main_panel.renderable, Table)
    assert main_panel.renderable is fake_table


def test_queues_presenter_dump_yaml_roundtrip():
    # Create queues using PBSQueue.fromDict
    info_gpu = {
        "queue_type": "Execution",
        "Priority": "75",
        "total_jobs": "367",
        "state_count": "Transit:0 Queued:235 Held:0 Waiting:0 Running:132 Exiting:0 Begun:0",
        "resources_max.ngpus": "99",
        "resources_max.walltime": "24:00:00",
        "comment": "Queue for jobs computed on GPU",
        "enabled": "True",
        "started": "True",
    }
    info_cpu = {
        "queue_type": "Execution",
        "Priority": "100",
        "total_jobs": "120",
        "state_count": "Transit:0 Queued:50 Held:0 Waiting:0 Running:70 Exiting:0 Begun:0",
        "resources_max.walltime": "12:00:00",
        "comment": "Queue for CPU jobs",
        "enabled": "True",
        "started": "True",
    }

    gpu_queue = PBSQueue.fromDict("gpu", info_gpu)
    cpu_queue = PBSQueue.fromDict("cpu", info_cpu)

    presenter = QueuesPresenter([gpu_queue, cpu_queue], user="user", all=True)

    captured = StringIO()
    sys.stdout = captured
    try:
        presenter.dumpYaml()
    finally:
        sys.stdout = sys.__stdout__

    yaml_output = captured.getvalue().strip().split("\n\n")
    reloaded_queues = []

    for doc in yaml_output:
        if not doc.strip():
            continue
        data = yaml.safe_load(doc)
        name = data["Queue"]
        reloaded_queues.append(PBSQueue.fromDict(name, data))

    # check that both queues were dumped and reloaded
    assert len(reloaded_queues) == 2

    for orig, loaded in zip([gpu_queue, cpu_queue], reloaded_queues):
        assert orig.getName() == loaded.getName()
        assert orig.getPriority() == loaded.getPriority()
        assert orig.getTotalJobs() == loaded.getTotalJobs()
        assert orig.getRunningJobs() == loaded.getRunningJobs()
        assert orig.getQueuedJobs() == loaded.getQueuedJobs()
        assert orig.getComment() == loaded.getComment()


def test_queues_presenter_should_show_comment_returns_true_if_any_has_comment():
    q1 = _make_queue("queue1")
    q1.getComment.return_value = None

    q2 = _make_queue("queue2")

    presenter = QueuesPresenter([q1, q2], user="user", all=False)
    assert presenter._shouldShowComment()


def test_queues_presenter_should_show_comment_returns_true_if_none_has_comment():
    q1 = _make_queue("queue1")
    q1.getComment.return_value = None

    q2 = _make_queue("queue2")
    q2.getComment.return_value = None

    presenter = QueuesPresenter([q1, q2], user="user", all=False)
    assert not presenter._shouldShowComment()


def test_queues_presenter_should_show_max_nodes_returns_true_if_any_has_max_nodes():
    q1 = _make_queue("queue1", nnodes=None)

    q2 = _make_queue("queue2")

    presenter = QueuesPresenter([q1, q2], user="user", all=False)
    assert presenter._shouldShowMaxNNodes()


def test_queues_presenter_should_show_max_nodes_returns_false_if_none_have_max_nodes():
    q1 = _make_queue("queue1", nnodes=None)
    q2 = _make_queue("queue2", nnodes=None)

    presenter = QueuesPresenter([q1, q2], user="user", all=False)
    assert not presenter._shouldShowMaxNNodes()
