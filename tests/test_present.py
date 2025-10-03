# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table

from qq_lib.constants import DATE_FORMAT
from qq_lib.info import QQInfo, QQInformer
from qq_lib.job_type import QQJobType
from qq_lib.pbs import QQPBS
from qq_lib.present import QQPresenter
from qq_lib.resources import QQResources
from qq_lib.states import NaiveState, RealState


@pytest.fixture
def sample_resources():
    return QQResources(
        nnodes=1,
        ncpus=8,
        work_dir="scratch_local",
        ngpus=1,
        props="cl_cluster,^infiniband,vnode=^faulty_node",
    )


@pytest.fixture
def sample_info(sample_resources):
    return QQInfo(
        batch_system=QQPBS,
        qq_version="0.1.0",
        username="fake_user",
        job_id="12345.fake.server.com",
        job_name="script.sh+025",
        queue="default",
        script_name="script.sh",
        job_type=QQJobType.STANDARD,
        input_machine="fake.machine.com",
        job_dir=Path("/shared/storage/"),
        job_state=NaiveState.RUNNING,
        submission_time=datetime.strptime("2025-09-21 12:00:00", DATE_FORMAT),
        stdout_file="stdout.log",
        stderr_file="stderr.log",
        resources=sample_resources,
        excluded_files=[Path("ignore.txt")],
        command_line=["-q", "default", "script.sh"],
        main_node="random.node.org",
        work_dir=Path("/scratch/job_12345.fake.server.com"),
    )


@pytest.mark.parametrize(
    "state,expected_first_keyword,expected_second_keyword",
    [
        (RealState.QUEUED, "queued", "queue"),
        (RealState.HELD, "held", "queue"),
        (RealState.SUSPENDED, "suspended", ""),
        (RealState.WAITING, "waiting", "queue"),
        (RealState.RUNNING, "running", "running"),
        (RealState.BOOTING, "booting", "preparing"),
        (RealState.KILLED, "killed", "killed"),
        (RealState.FAILED, "failed", "failed"),
        (RealState.FINISHED, "finished", "completed"),
        (RealState.IN_AN_INCONSISTENT_STATE, "inconsistent", "disagree"),
        (RealState.UNKNOWN, "unknown", "does not recognize"),
    ],
)
def test_presenter_state_messages(
    sample_info, state, expected_first_keyword, expected_second_keyword
):
    # set required fields for running/finished/failed states
    if state == RealState.RUNNING:
        sample_info.main_node = "node1"

    if state == RealState.FAILED:
        sample_info.job_exit_code = 1

    presenter = QQPresenter(QQInformer(sample_info))

    start_time = datetime.now()
    end_time = start_time + timedelta(hours=1)

    first_msg, second_msg = presenter._getStateMessages(state, start_time, end_time)

    assert expected_first_keyword.lower() in first_msg.lower()
    assert expected_second_keyword.lower() in second_msg.lower()


def test_create_job_status_panel(sample_info):
    presenter = QQPresenter(QQInformer(sample_info))

    with patch.object(QQInformer, "getRealState", return_value=RealState.RUNNING):
        panel_group: Group = presenter.createJobStatusPanel()

    # group
    assert isinstance(panel_group, Group)
    assert len(panel_group.renderables) == 3

    # panel
    panel: Panel = panel_group.renderables[1]
    assert isinstance(panel, Panel)
    assert presenter._informer.info.job_id in panel.title.plain

    # table
    table: Table = panel.renderable
    assert isinstance(table, Table)
    assert len(table.columns) == 2

    # printed content
    console = Console(record=True)
    console.print(table)
    output = console.export_text()

    assert "Job state:" in output
    assert str(RealState.RUNNING).lower() in output.lower()


def test_create_basic_info_table(sample_info):
    presenter = QQPresenter(QQInformer(sample_info))
    table = presenter._createBasicInfoTable()

    assert isinstance(table, Table)
    assert len(table.columns) == 2

    console = Console(record=True)
    console.print(table)
    output = console.export_text()

    assert "Job name:" in output
    assert sample_info.job_name in output
    assert "Job type:" in output
    assert str(sample_info.job_type) in output
    assert "Submission queue:" in output
    assert sample_info.queue in output
    assert "Input machine:" in output
    assert sample_info.input_machine in output
    assert "Input directory:" in output
    assert str(sample_info.job_dir) in output
    assert "Main working node:" in output
    assert str(sample_info.main_node) in output
    assert "Working directory:" in output
    assert str(sample_info.work_dir) in output


def test_create_basic_info_table_no_working(sample_info):
    sample_info.main_node = None
    sample_info.work_dir = None

    presenter = QQPresenter(QQInformer(sample_info))
    table = presenter._createBasicInfoTable()

    assert isinstance(table, Table)
    assert len(table.columns) == 2

    console = Console(record=True)
    console.print(table)
    output = console.export_text()

    assert "Job name:" in output
    assert sample_info.job_name in output
    assert "Submission queue:" in output
    assert sample_info.queue in output
    assert "Input machine:" in output
    assert sample_info.input_machine in output
    assert "Input directory:" in output
    assert str(sample_info.job_dir) in output

    assert "Main working node:" not in output
    assert "Working directory:" not in output


def test_create_resources_table(sample_info):
    console = Console(record=True)
    presenter = QQPresenter(QQInformer(sample_info))
    table = presenter._createResourcesTable(term_width=console.size.width)

    assert isinstance(table, Table)
    assert len(table.columns) == 5

    console.print(table)
    output = console.export_text()

    assert "nnodes:" in output
    assert str(sample_info.resources.nnodes) in output
    assert "ncpus:" in output
    assert str(sample_info.resources.ncpus) in output
    assert "ngpus:" in output
    assert str(sample_info.resources.ngpus) in output
    assert "work-dir:" in output
    assert str(sample_info.resources.work_dir) in output
    assert "cl_cluster:" in output
    assert "infiniband:" in output
    assert "vnode:" in output
    assert "^faulty_node" in output


def test_create_job_history_table_with_times(sample_info):
    # add start and completion times
    sample_info.start_time = sample_info.submission_time + timedelta(minutes=10)
    sample_info.completion_time = sample_info.start_time + timedelta(minutes=30)

    presenter = QQPresenter(QQInformer(sample_info))
    table = presenter._createJobHistoryTable()

    assert isinstance(table, Table)
    assert len(table.columns) == 2

    console = Console(record=True)
    console.print(table)
    output = console.export_text()

    assert "Submitted at:" in output
    assert str(sample_info.submission_time) in output
    assert "was queued" in output
    assert "Started at:" in output
    assert str(sample_info.start_time) in output
    assert "was running" in output
    assert "Completed at:" in output
    assert str(sample_info.completion_time) in output


def test_create_job_history_table_submitted_only(sample_info):
    # no start or completion time set
    sample_info.start_time = None
    sample_info.completion_time = None

    presenter = QQPresenter(QQInformer(sample_info))
    table = presenter._createJobHistoryTable()

    console = Console(record=True)
    console.print(table)
    output = console.export_text()

    assert "Submitted at:" in output
    assert str(sample_info.submission_time) in output
    assert "Started at:" not in output
    assert "finished" not in output.lower()


@pytest.mark.parametrize("state", list(RealState))
def test_create_job_status_table_states(sample_info, state):
    # prepare info for special states
    if state == RealState.RUNNING:
        sample_info.start_time = sample_info.submission_time + timedelta(seconds=10)
        sample_info.main_node = "node1"
    if state == RealState.FINISHED:
        sample_info.start_time = sample_info.submission_time + timedelta(seconds=10)
        sample_info.completion_time = sample_info.start_time + timedelta(seconds=20)
    if state == RealState.FAILED:
        sample_info.job_exit_code = 1
        sample_info.start_time = sample_info.submission_time + timedelta(seconds=10)
        sample_info.completion_time = sample_info.start_time + timedelta(seconds=20)

    informer = QQInformer(sample_info)
    informer.info.job_state = state
    presenter = QQPresenter(informer)

    table = presenter._createJobStatusTable(state)

    assert isinstance(table, Table)
    assert len(table.columns) == 2

    console = Console(record=True)
    console.print(table)
    output = console.export_text()

    assert "Job state:" in output
    first_msg, second_msg = presenter._getStateMessages(
        state,
        sample_info.start_time or sample_info.submission_time,
        sample_info.completion_time or datetime.now(),
    )
    assert first_msg in output
    assert second_msg in output


@pytest.mark.parametrize(
    "state", [RealState.QUEUED, RealState.HELD, RealState.SUSPENDED, RealState.WAITING]
)
def test_create_job_status_table_with_estimated(sample_info, state):
    informer = QQInformer(sample_info)
    informer.info.job_state = state
    presenter = QQPresenter(informer)

    table = presenter._createJobStatusTable(
        state, "Should not be printed", (datetime.now(), "fake_node")
    )

    assert isinstance(table, Table)
    assert len(table.columns) == 2

    console = Console(record=True)
    console.print(table)
    output = console.export_text()

    assert "Job state:" in output
    first_msg, second_msg = presenter._getStateMessages(
        state,
        sample_info.start_time or sample_info.submission_time,
        sample_info.completion_time or datetime.now(),
    )
    assert first_msg in output
    assert second_msg in output
    assert "Planned start within" in output
    assert "fake_node" in output
    assert "Should not be printed" not in output


@pytest.mark.parametrize(
    "state", [RealState.QUEUED, RealState.HELD, RealState.SUSPENDED, RealState.WAITING]
)
def test_create_job_status_table_with_comment(sample_info, state):
    informer = QQInformer(sample_info)
    informer.info.job_state = state
    presenter = QQPresenter(informer)

    table = presenter._createJobStatusTable(state, "This is a test comment")

    assert isinstance(table, Table)
    assert len(table.columns) == 2

    console = Console(record=True)
    console.print(table)
    output = console.export_text()

    assert "Job state:" in output
    first_msg, second_msg = presenter._getStateMessages(
        state,
        sample_info.start_time or sample_info.submission_time,
        sample_info.completion_time or datetime.now(),
    )
    assert first_msg in output
    assert second_msg in output
    assert "This is a test comment" in output


@pytest.fixture
def mock_informer():
    informer = Mock()
    # default return values
    informer.getComment.return_value = "Job comment"
    informer.getEstimated.return_value = (datetime(2026, 10, 4, 15, 30, 0), "node01")
    return informer


@pytest.fixture
def presenter(mock_informer):
    return QQPresenter(mock_informer)


@pytest.mark.parametrize(
    "state",
    [
        RealState.QUEUED,
        RealState.HELD,
        RealState.WAITING,
        RealState.SUSPENDED,
    ],
)
def test_get_comment_and_estimated_for_active_states(presenter, mock_informer, state):
    comment, estimated = presenter._getCommentAndEstimated(state)

    # check that the values returned are what the informer provides
    assert comment == "Job comment"
    assert estimated == (datetime(2026, 10, 4, 15, 30, 0), "node01")

    # check that the presenter actually called the informer methods
    mock_informer.getComment.assert_called_once()
    mock_informer.getEstimated.assert_called_once()


@pytest.mark.parametrize(
    "state",
    [
        RealState.BOOTING,
        RealState.RUNNING,
        RealState.FINISHED,
        RealState.FAILED,
        RealState.KILLED,
        RealState.UNKNOWN,
        RealState.IN_AN_INCONSISTENT_STATE,
    ],
)
def test_get_comment_and_estimated_for_inactive_states(presenter, state):
    comment, estimated = presenter._getCommentAndEstimated(state)
    assert comment is None
    assert estimated is None
