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
from qq_lib.queues.presenter import QQQueuesPresenter


def test_qqqueues_presenter_init_sets_fields_correctly():
    queues = [MagicMock(), MagicMock()]
    user = "user"
    display_all = True

    presenter = QQQueuesPresenter(queues, user, display_all)

    assert presenter._queues == queues
    assert presenter._user == user
    assert presenter._display_all is True


def test_qqqueues_presenter_format_walltime_returns_formatted_text():
    queue = MagicMock()
    queue.getMaxWalltime.return_value = timedelta(days=1, hours=2, minutes=3, seconds=4)

    result = QQQueuesPresenter._formatWalltime(queue, "cyan")

    assert isinstance(result, Text)
    assert result.plain == "1d 02:03:04"
    assert result.style == "cyan"
    queue.getMaxWalltime.assert_called_once()


def test_qqqueues_presenter_format_walltime_returns_empty_text_when_no_walltime():
    queue = MagicMock()
    queue.getMaxWalltime.return_value = None

    result = QQQueuesPresenter._formatWalltime(queue, "cyan")

    assert isinstance(result, Text)
    assert result.plain == ""
    queue.getMaxWalltime.assert_called_once()


def test_qqqueues_presenter_add_queue_row_main_available():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = True
    queue.getName.return_value = "mainq"
    queue.getPriority.return_value = 47
    queue.getRunningJobs.return_value = 5
    queue.getQueuedJobs.return_value = 3
    queue.getOtherJobs.return_value = 2
    queue.getTotalJobs.return_value = 10
    queue.getComment.return_value = "Main queue"
    queue.getMaxWalltime.return_value = None

    table = Table()
    QQQueuesPresenter._addQueueRow(queue, table, user="user")

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
    assert "Main queue" in output
    queue.isAvailableToUser.assert_called_once_with("user")


def test_qqqueues_presenter_add_queue_row_main_unavailable():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = False
    queue.getName.return_value = "main_unavail"
    queue.getPriority.return_value = 0
    queue.getRunningJobs.return_value = 0
    queue.getQueuedJobs.return_value = 1
    queue.getOtherJobs.return_value = 0
    queue.getTotalJobs.return_value = 1
    queue.getComment.return_value = "No access"
    queue.getMaxWalltime.return_value = None

    table = Table()
    QQQueuesPresenter._addQueueRow(queue, table, user="user")

    buffer = StringIO()
    console = Console(file=buffer, width=160, force_terminal=False, color_system=None)
    console.print(table)
    output = buffer.getvalue()

    assert "main_unavail" in output
    assert "0" in output
    assert "1" in output
    assert "No access" in output
    queue.isAvailableToUser.assert_called_once_with("user")


def test_qqqueues_presenter_add_queue_row_rerouted_available():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = True
    queue.getName.return_value = "reroutedq"
    queue.getPriority.return_value = 7
    queue.getRunningJobs.return_value = 2
    queue.getQueuedJobs.return_value = 4
    queue.getOtherJobs.return_value = 1
    queue.getTotalJobs.return_value = 7
    queue.getComment.return_value = "Rerouted ok"
    queue.getMaxWalltime.return_value = None

    table = Table()
    QQQueuesPresenter._addQueueRow(queue, table, user="user", from_route=True)

    buffer = StringIO()
    console = Console(file=buffer, width=160, force_terminal=False, color_system=None)
    console.print(table)
    output = buffer.getvalue()

    assert "reroutedq" in output
    assert "7" in output
    assert "2" in output
    assert "4" in output
    assert "1" in output
    assert "Rerouted ok" in output
    assert CFG.queues_presenter.rerouted_mark in output
    queue.isAvailableToUser.assert_called_once_with("user")


def test_qqqueues_presenter_add_queue_row_rerouted_unavailable():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = False
    queue.getName.return_value = "rerouted_blocked"
    queue.getPriority.return_value = 3
    queue.getRunningJobs.return_value = 0
    queue.getQueuedJobs.return_value = 0
    queue.getOtherJobs.return_value = 0
    queue.getTotalJobs.return_value = 0
    queue.getComment.return_value = "Rerouted blocked"
    queue.getMaxWalltime.return_value = None

    table = Table()
    QQQueuesPresenter._addQueueRow(queue, table, user="user", from_route=True)

    buffer = StringIO()
    console = Console(file=buffer, width=160, force_terminal=False, color_system=None)
    console.print(table)
    output = buffer.getvalue()

    assert "rerouted_blocked" in output
    assert "3" in output
    assert "Rerouted blocked" in output
    assert CFG.queues_presenter.rerouted_mark in output
    queue.isAvailableToUser.assert_called_once_with("user")


def test_qqqueues_presenter_add_queue_row_dangling():
    queue = MagicMock()
    queue.isAvailableToUser.return_value = True
    queue.getName.return_value = "danglingq"
    queue.getPriority.return_value = 11
    queue.getRunningJobs.return_value = 1
    queue.getQueuedJobs.return_value = 1
    queue.getOtherJobs.return_value = 1
    queue.getTotalJobs.return_value = 3
    queue.getComment.return_value = "Dangling dest"
    queue.getMaxWalltime.return_value = None

    table = Table()
    QQQueuesPresenter._addQueueRow(queue, table, user="user", dangling=True)

    buffer = StringIO()
    console = Console(file=buffer, width=160, force_terminal=False, color_system=None)
    console.print(table)
    output = buffer.getvalue()

    assert "danglingq" in output
    assert "11" in output
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
    priority: int | None = 10,
    running: int = 1,
    queued: int = 2,
    other: int = 3,
    total: int = 6,
    comment: str = "comment",
    walltime: object = None,
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
    return q


def _render_table(table: Table) -> str:
    buf = StringIO()
    Console(file=buf, width=200, force_terminal=False, color_system=None).print(table)
    return buf.getvalue()


def test_qqqueues_presenter_create_queues_table_basic_main_only():
    main = _make_queue("mainq", destinations=[])
    presenter = QQQueuesPresenter([main], user="user", all=False)

    table = presenter._createQueuesTable()
    output = _render_table(table)

    assert "Name" in output
    assert "Priority" in output
    assert CFG.queues_presenter.main_mark in output
    assert "mainq" in output
    main.isAvailableToUser.assert_called_once_with("user")


def test_qqqueues_presenter_create_queues_table_with_rerouted_parent_available():
    main = _make_queue(
        "mainq", destinations=["destq"], available_to=True, comment="main"
    )
    dest = _make_queue("destq", comment="dest")
    presenter = QQQueuesPresenter([main, dest], user="user", all=False)

    table = presenter._createQueuesTable()
    output = _render_table(table)

    assert CFG.queues_presenter.main_mark in output
    assert CFG.queues_presenter.rerouted_mark in output
    assert "mainq" in output
    assert "destq" in output


def test_qqqueues_presenter_create_queues_table_with_rerouted_parent_unavailable():
    main = _make_queue("mainq", destinations=["destq"], available_to=False)
    dest = _make_queue("destq")
    presenter = QQQueuesPresenter([main, dest], user="user", all=False)

    table = presenter._createQueuesTable()
    output = _render_table(table)

    assert "mainq" in output
    assert "destq" in output
    assert CFG.queues_presenter.main_mark in output
    assert CFG.queues_presenter.rerouted_mark in output


def test_qqqueues_presenter_create_queues_table_unbound_when_all_true():
    route_only_unbound = _make_queue(
        "lonely_dest", from_route_only=True, comment="dangling"
    )
    presenter = QQQueuesPresenter([route_only_unbound], user="user", all=True)

    table = presenter._createQueuesTable()
    output = _render_table(table)

    # dangling mark row indicator and the unbound queue should be printed
    assert "?" in output
    assert "lonely_dest" in output
    route_only_unbound.isAvailableToUser.assert_called_once_with("user")


@pytest.mark.parametrize("all", [False, True])
def test_qqqueues_presenter_create_queues_info_panel_structure(all):
    queue_mock = MagicMock()
    presenter = QQQueuesPresenter([queue_mock], user="user", all=all)

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


def test_qqqueues_presenter_dump_yaml_roundtrip():
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

    presenter = QQQueuesPresenter([gpu_queue, cpu_queue], user="user", all=True)

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
