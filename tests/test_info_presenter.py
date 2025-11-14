# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from rich.console import Console, Group
from rich.padding import Padding
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from qq_lib.batch.pbs import PBS
from qq_lib.info.informer import Informer
from qq_lib.info.presenter import CFG, Presenter
from qq_lib.properties.info import Info
from qq_lib.properties.job_type import JobType
from qq_lib.properties.resources import Resources
from qq_lib.properties.states import BatchState, NaiveState, RealState


@pytest.fixture
def sample_resources():
    return Resources(
        nnodes=1,
        ncpus=8,
        work_dir="scratch_local",
        ngpus=1,
        props="cl_cluster,^infiniband,vnode=^faulty_node",
    )


@pytest.fixture
def sample_info(sample_resources):
    return Info(
        batch_system=PBS,
        qq_version="0.1.0",
        username="fake_user",
        job_id="12345.fake.server.com",
        job_name="script.sh+025",
        queue="default",
        script_name="script.sh",
        job_type=JobType.STANDARD,
        input_machine="fake.machine.com",
        input_dir=Path("/shared/storage/"),
        job_state=NaiveState.RUNNING,
        submission_time=datetime.strptime(
            "2025-09-21 12:00:00", CFG.date_formats.standard
        ),
        stdout_file="stdout.log",
        stderr_file="stderr.log",
        resources=sample_resources,
        excluded_files=[Path("ignore.txt")],
        command_line=["-q", "default", "script.sh"],
        main_node="random.node.org",
        all_nodes=["random.node.org"],
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

    presenter = Presenter(Informer(sample_info))

    start_time = datetime.now()
    end_time = start_time + timedelta(hours=1)

    first_msg, second_msg = presenter._getStateMessages(state, start_time, end_time)

    assert expected_first_keyword.lower() in first_msg.lower()
    assert expected_second_keyword.lower() in second_msg.lower()


def test_presenter_state_messages_running_single_node(sample_info):
    sample_info.main_node = ["node1"]
    sample_info.all_nodes = ["node1"]

    presenter = Presenter(Informer(sample_info))

    start_time = datetime.now()
    end_time = start_time + timedelta(hours=1)

    first_msg, second_msg = presenter._getStateMessages(
        RealState.RUNNING, start_time, end_time
    )

    assert "running" in first_msg.lower()
    assert "node1" in second_msg.lower()
    assert "and" not in second_msg.lower()
    assert "other nodes" not in second_msg.lower()


def test_presenter_state_messages_running_multiple_nodes(sample_info):
    sample_info.main_node = ["node1"]
    sample_info.all_nodes = ["node1", "node3", "node4", "node2"]

    presenter = Presenter(Informer(sample_info))

    start_time = datetime.now()
    end_time = start_time + timedelta(hours=1)

    first_msg, second_msg = presenter._getStateMessages(
        RealState.RUNNING, start_time, end_time
    )

    assert "running" in first_msg.lower()
    assert "node1" in second_msg.lower()
    assert "and 3 other nodes" in second_msg.lower()


@pytest.mark.parametrize("exit_code", [None, 0, 3])
def test_presenter_state_messages_exiting(sample_info, exit_code):
    sample_info.job_exit_code = exit_code

    presenter = Presenter(Informer(sample_info))
    first_msg, second_msg = presenter._getStateMessages(
        RealState.EXITING, datetime.now(), datetime.now()
    )

    assert "exiting" in first_msg
    if exit_code is None:
        assert "killed" in second_msg
    elif exit_code == 0:
        assert "finishing" in second_msg
    else:
        assert "failing" in second_msg


def test_create_job_status_panel(sample_info):
    presenter = Presenter(Informer(sample_info))

    with patch.object(Informer, "getRealState", return_value=RealState.RUNNING):
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
    presenter = Presenter(Informer(sample_info))
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
    assert str(sample_info.input_dir) in output
    assert "Working node:" in output
    assert str(sample_info.main_node) in output
    assert "Working directory:" in output
    assert str(sample_info.work_dir) in output


def test_create_basic_info_table_multiple_nodes(sample_info):
    sample_info.all_nodes = ["node01", "nod04", "node02", "node06"]
    presenter = Presenter(Informer(sample_info))
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
    assert str(sample_info.input_dir) in output
    assert "Working nodes:" in output
    assert " + ".join(sample_info.all_nodes) in output
    assert "Working directory:" in output
    assert str(sample_info.work_dir) in output


def test_create_basic_info_table_no_working(sample_info):
    sample_info.main_node = None
    sample_info.work_dir = None

    presenter = Presenter(Informer(sample_info))
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
    assert str(sample_info.input_dir) in output

    assert "Main working node:" not in output
    assert "Working directory:" not in output


def test_create_resources_table(sample_info):
    console = Console(record=True)
    presenter = Presenter(Informer(sample_info))
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


@pytest.mark.parametrize(
    "state, exit_code",
    [
        (RealState.FINISHED, 0),
        (RealState.FAILED, 1),
        (RealState.KILLED, None),
        (RealState.EXITING, 0),
        (RealState.EXITING, 1),
        (RealState.EXITING, None),
    ],
)
def test_create_job_history_table_with_times(sample_info, state, exit_code):
    # add start and completion times
    sample_info.start_time = sample_info.submission_time + timedelta(minutes=10)
    sample_info.completion_time = sample_info.start_time + timedelta(minutes=30)

    presenter = Presenter(Informer(sample_info))
    table = presenter._createJobHistoryTable(state, exit_code)

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
    assert (
        f"{Presenter._translateStateToCompletedMsg(state, exit_code).title()} at:"
        in output
    )
    assert str(sample_info.completion_time) in output


def test_create_job_history_table_submitted_only(sample_info):
    # no start or completion time set
    sample_info.start_time = None
    sample_info.completion_time = None

    presenter = Presenter(Informer(sample_info))
    table = presenter._createJobHistoryTable(RealState.QUEUED, None)

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

    informer = Informer(sample_info)
    informer.info.job_state = state
    presenter = Presenter(informer)

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
    informer = Informer(sample_info)
    informer.info.job_state = state
    presenter = Presenter(informer)

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
    informer = Informer(sample_info)
    informer.info.job_state = state
    presenter = Presenter(informer)

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
    informer.getEstimated.return_value = (datetime(2124, 10, 4, 15, 30, 0), "node01")
    return informer


@pytest.fixture
def presenter(mock_informer):
    return Presenter(mock_informer)


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
    assert estimated == (datetime(2124, 10, 4, 15, 30, 0), "node01")

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


@pytest.mark.parametrize("state", list(RealState))
def test_get_short_info_returns_correct_text_and_style(state):
    informer_mock = Mock()
    informer_mock.info.job_id = "12345"
    informer_mock.getRealState.return_value = state

    presenter = Presenter(informer_mock)

    result = presenter.getShortInfo()

    assert isinstance(result, Text)
    text_str = str(result)
    assert "12345" in text_str
    assert str(state) in text_str

    assert any(span.style == state.color for span in result.spans)

    informer_mock.getRealState.assert_called_once()


def test_get_short_info_combines_job_id_and_state_correctly():
    informer_mock = Mock()
    informer_mock.info.job_id = "9999"
    informer_mock.getRealState.return_value = RealState.RUNNING

    presenter = Presenter(informer_mock)

    result = presenter.getShortInfo()

    assert str(result) == "9999    running"
    assert any(span.style == RealState.RUNNING.color for span in result.spans)


@pytest.mark.parametrize(
    "state, exit_code, expected",
    [
        (RealState.FINISHED, 0, "finished"),
        (RealState.FAILED, 1, "failed"),
        (RealState.KILLED, None, "killed"),
        (RealState.EXITING, 0, "finished"),
        (RealState.EXITING, 1, "failed"),
        (RealState.EXITING, 42, "failed"),
        (RealState.EXITING, None, "killed"),
        (RealState.RUNNING, None, "completed"),
    ],
)
def test_translate_state_to_completed_msg(state, exit_code, expected):
    assert Presenter._translateStateToCompletedMsg(state, exit_code) == expected


def test_presenter_create_job_steps_block_returns_empty_group_when_no_steps():
    informer = MagicMock()
    job = MagicMock()
    job.getSteps.return_value = []
    informer.getBatchInfo.return_value = job

    presenter = Presenter(informer)

    result = presenter._createJobStepsBlock()

    assert isinstance(result, Group)
    assert len(result.renderables) == 0


def test_presenter_create_job_steps_block_returns_empty_group_when_one_step():
    informer = MagicMock()
    job = MagicMock()
    job.getSteps.return_value = [MagicMock()]
    informer.getBatchInfo.return_value = job

    presenter = Presenter(informer)

    result = presenter._createJobStepsBlock()

    assert isinstance(result, Group)
    assert len(result.renderables) == 0


def test_presenter_create_job_steps_block_returns_full_block_for_multiple_steps():
    informer = MagicMock()
    job = MagicMock()
    job.getSteps.return_value = [MagicMock(), MagicMock()]
    informer.getBatchInfo.return_value = job

    presenter = Presenter(informer)

    with patch.object(presenter, "_createJobStepsTable", return_value="TABLE"):
        result = presenter._createJobStepsBlock()

    assert isinstance(result, Group)
    assert len(result.renderables) == 4

    assert isinstance(result.renderables[0], Text)
    assert result.renderables[0].plain == ""

    assert isinstance(result.renderables[1], Rule)
    assert isinstance(result.renderables[1].title, Text)
    assert result.renderables[1].title.plain == "STEPS"

    assert isinstance(result.renderables[2], Text)
    assert result.renderables[2].plain == ""

    assert isinstance(result.renderables[3], Padding)
    assert result.renderables[3].renderable == "TABLE"


def test_presenter_create_job_steps_table_adds_rows_for_valid_steps():
    informer = MagicMock()
    presenter = Presenter(informer)

    start = datetime(2025, 1, 1, 10, 0, 0)
    end = datetime(2025, 1, 1, 12, 0, 0)

    step = MagicMock()
    step.getState.return_value = BatchState.RUNNING
    step.getStartTime.return_value = start
    step.getCompletionTime.return_value = end
    step.getStepId.return_value = "1"

    table = presenter._createJobStepsTable([step])

    console = Console(record=True)
    console.print(table)
    output = console.export_text()

    assert "1" in output
    assert "R" in output
    assert start.strftime("%Y-%m-%d %H:%M:%S") in output
    assert end.strftime("%Y-%m-%d %H:%M:%S") in output

    assert "02:00:00" in output


def test_presenter_create_job_steps_table_uses_now_when_end_missing():
    informer = MagicMock()
    presenter = Presenter(informer)

    start = datetime(2025, 1, 1, 10, 0, 0)
    fake_now = datetime(2025, 1, 1, 12, 0, 0)

    step = MagicMock()
    step.getState.return_value = BatchState.RUNNING
    step.getStartTime.return_value = start
    step.getCompletionTime.return_value = None
    step.getStepId.return_value = "1"

    with patch("qq_lib.info.presenter.datetime") as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        table = presenter._createJobStepsTable([step])

    console = Console(record=True)
    console.print(table)
    output = console.export_text()

    assert "1" in output
    assert "R" in output
    assert start.strftime("%Y-%m-%d %H:%M:%S") in output
    assert fake_now.strftime("%Y-%m-%d %H:%M:%S") not in output

    assert "02:00:00" in output
