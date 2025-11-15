# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import io
import sys
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest
import yaml
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text

from qq_lib.batch.pbs import PBSJob
from qq_lib.batch.pbs.common import parse_multi_pbs_dump_to_dictionaries
from qq_lib.batch.pbs.pbs import PBS
from qq_lib.core.common import format_duration_wdhhmmss
from qq_lib.jobs.presenter import CFG, JobsPresenter, JobsStatistics
from qq_lib.properties.states import BatchState


def test_init_sets_all_attributes_and_creates_statistics():
    job1 = Mock()
    job2 = Mock()
    jobs = [job1, job2]

    with patch("qq_lib.jobs.presenter.JobsStatistics") as mock_stats_class:
        mock_stats_instance = Mock()
        mock_stats_class.return_value = mock_stats_instance

        presenter = JobsPresenter(PBS, jobs, True, False)

    assert presenter._batch_system == PBS
    assert presenter._jobs == jobs
    assert presenter._extra is True
    assert presenter._all is False
    assert presenter._stats == mock_stats_instance
    mock_stats_class.assert_called_once_with()


@pytest.mark.parametrize(
    "string,color,bold,expected_prefix",
    [
        ("test", None, False, ""),  # no color, no bold
        ("test", "red", False, JobsPresenter.ANSI_COLORS["red"]),
        ("test", None, True, JobsPresenter.ANSI_COLORS["bold"]),
        (
            "test",
            "green",
            True,
            JobsPresenter.ANSI_COLORS["bold"] + JobsPresenter.ANSI_COLORS["green"],
        ),
    ],
)
def test_color_applies_correct_ansi(string, color, bold, expected_prefix):
    result = JobsPresenter._color(string, color=color, bold=bold)
    reset = JobsPresenter.ANSI_COLORS["reset"] if color or bold else ""
    assert result == f"{expected_prefix}{string}{reset}"


def test_main_color_applies_main_color():
    text = "text"
    result = JobsPresenter._mainColor(text)
    expected = f"{JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.main_style]}text{JobsPresenter.ANSI_COLORS['reset']}"
    assert result == expected


def test_main_color_applies_main_color_and_bold():
    text = "text"
    result = JobsPresenter._mainColor(text, bold=True)
    expected = f"{JobsPresenter.ANSI_COLORS['bold']}{JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.main_style]}text{JobsPresenter.ANSI_COLORS['reset']}"
    assert result == expected


def test_secondary_color_applies_secondary_color():
    text = "text"
    result = JobsPresenter._secondaryColor(text)
    expected = f"{JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.secondary_style]}text{JobsPresenter.ANSI_COLORS['reset']}"
    assert result == expected


def test_secondary_color_applies_secondary_color_and_bold():
    text = "text"
    result = JobsPresenter._secondaryColor(text, bold=True)
    expected = f"{JobsPresenter.ANSI_COLORS['bold']}{JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.secondary_style]}text{JobsPresenter.ANSI_COLORS['reset']}"
    assert result == expected


@pytest.mark.parametrize(
    "input_id,expected",
    [
        ("12345", "12345"),
        ("12345.6789", "12345"),
        ("abc.def.ghi", "abc"),
        (".leadingdot", ""),
        ("trailingdot.", "trailingdot"),
        ("..doubleleading", ""),
        ("middle.dot.example", "middle"),
        ("", ""),
    ],
)
def test_shorten_job_id(input_id, expected):
    assert JobsPresenter._shortenJobId(input_id) == expected


@pytest.fixture
def mock_job():
    return PBSJob.__new__(PBSJob)


def test_format_nodes_or_comment_returns_single_node(mock_job):
    with (
        patch.object(mock_job, "getShortNodes", return_value=["node1"]),
        patch.object(mock_job, "getEstimated", return_value=None),
    ):
        result = JobsPresenter._formatNodesOrComment(BatchState.RUNNING, mock_job)
        expected = JobsPresenter._mainColor("node1")
        assert result == expected


def test_format_nodes_or_comment_returns_nodes(mock_job):
    with (
        patch.object(mock_job, "getShortNodes", return_value=["node1", "node2"]),
        patch.object(mock_job, "getEstimated", return_value=None),
    ):
        result = JobsPresenter._formatNodesOrComment(BatchState.RUNNING, mock_job)
        expected = JobsPresenter._mainColor("node1 + node2")
        assert result == expected


def test_format_nodes_or_comment_returns_truncated_nodes(mock_job):
    with (
        patch.object(
            mock_job,
            "getShortNodes",
            return_value=[f"node{i}" for i in range(1, 10)],
        ),
        patch.object(mock_job, "getEstimated", return_value=None),
    ):
        result = JobsPresenter._formatNodesOrComment(BatchState.RUNNING, mock_job)
        expected = JobsPresenter._mainColor("node1 + node2 + node3 + node4 + node5 + …")
        assert result == expected


@pytest.mark.parametrize("state", [BatchState.FINISHED, BatchState.FAILED])
def test_format_nodes_or_comment_finished_or_failed_no_nodes(mock_job, state):
    with (
        patch.object(mock_job, "getShortNodes", return_value=[]),
        patch.object(mock_job, "getEstimated", return_value=None),
    ):
        result = JobsPresenter._formatNodesOrComment(state, mock_job)
        assert result == ""


@pytest.mark.parametrize("state", [BatchState.FINISHED, BatchState.FAILED])
def test_format_nodes_or_comment_finished_or_failed_single_node(mock_job, state):
    with (
        patch.object(mock_job, "getShortNodes", return_value=["node1"]),
        patch.object(mock_job, "getEstimated", return_value=None),
    ):
        result = JobsPresenter._formatNodesOrComment(state, mock_job)
        expected = JobsPresenter._mainColor("node1")
        assert result == expected


def test_format_nodes_or_comment_returns_estimated(mock_job):
    now = datetime.now()
    estimated_time = now + timedelta(hours=2)
    desc = "node01"

    with (
        patch.object(mock_job, "getShortNodes", return_value=[]),
        patch.object(mock_job, "getEstimated", return_value=(estimated_time, desc)),
    ):
        result = JobsPresenter._formatNodesOrComment(BatchState.QUEUED, mock_job)

        assert JobsPresenter.ANSI_COLORS[BatchState.QUEUED.color] in result
        assert desc in result
        duration_str = format_duration_wdhhmmss(estimated_time - datetime.now()).rsplit(
            ":", 1
        )[0]
        assert duration_str in result


def test_format_nodes_or_comment_returns_estimated_truncated(mock_job):
    now = datetime.now()
    estimated_time = now + timedelta(hours=2)
    desc = " + ".join([f"node{i}" for i in range(1, 10)])

    with (
        patch.object(mock_job, "getShortNodes", return_value=[]),
        patch.object(mock_job, "getEstimated", return_value=(estimated_time, desc)),
    ):
        result = JobsPresenter._formatNodesOrComment(BatchState.QUEUED, mock_job)

        assert JobsPresenter.ANSI_COLORS[BatchState.QUEUED.color] in result
        assert "node1 + node2 + node3 + node4 + node5 + …" in result
        duration_str = format_duration_wdhhmmss(estimated_time - datetime.now()).rsplit(
            ":", 1
        )[0]
        assert duration_str in result


def test_format_nodes_or_comment_returns_empty_when_no_info(mock_job):
    with (
        patch.object(mock_job, "getShortNodes", return_value=[]),
        patch.object(mock_job, "getEstimated", return_value=None),
    ):
        result = JobsPresenter._formatNodesOrComment(BatchState.QUEUED, mock_job)
        assert result == ""


@pytest.mark.parametrize("util", [101, 150, 300])
def test_format_util_cpu_above_100_uses_strong_warning(util):
    result = JobsPresenter._formatUtilCPU(util)
    color_code = JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.strong_warning_style]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(JobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("util", [80, 85, 99, 100])
def test_format_util_cpu_80_to_100_uses_main_color(util):
    result = JobsPresenter._formatUtilCPU(util)
    color_code = JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.main_style]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(JobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("util", [60, 61, 79])
def test_format_util_cpu_60_to_79_uses_mild_warning(util):
    result = JobsPresenter._formatUtilCPU(util)
    color_code = JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.mild_warning_style]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(JobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("util", [0, 10, 59])
def test_format_util_cpu_below_60_uses_strong_warning(util):
    result = JobsPresenter._formatUtilCPU(util)
    color_code = JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.strong_warning_style]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(JobsPresenter.ANSI_COLORS["reset"])


def test_format_util_mem_none_returns_empty():
    assert JobsPresenter._formatUtilMem(None) == ""


@pytest.mark.parametrize("util", [0, 50, 89])
def test_format_util_mem_below_90_uses_main_color(util):
    result = JobsPresenter._formatUtilMem(util)
    color_code = JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.main_style]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(JobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("util", [90, 95, 99])
def test_format_util_mem_90_to_99_uses_mild_warning(util):
    result = JobsPresenter._formatUtilMem(util)
    color_code = JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.mild_warning_style]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(JobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("util", [100, 110, 150])
def test_format_util_mem_100_or_more_uses_strong_warning(util):
    result = JobsPresenter._formatUtilMem(util)
    color_code = JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.strong_warning_style]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(JobsPresenter.ANSI_COLORS["reset"])


@pytest.fixture
def start_end_walltime():
    """Provide consistent start, end, and walltime values for tests."""
    start = datetime(2025, 1, 1, 12, 0, 0)
    end = datetime(2025, 1, 1, 13, 0, 0)
    walltime = timedelta(hours=2)
    return start, end, walltime


@pytest.mark.parametrize("state", [BatchState.UNKNOWN, BatchState.SUSPENDED])
def test_format_time_unknown_or_suspended_returns_empty(state, start_end_walltime):
    start, end, walltime = start_end_walltime
    result = JobsPresenter._formatTime(state, start, end, walltime)
    assert result == ""


@pytest.mark.parametrize("state", [BatchState.FINISHED, BatchState.FAILED])
def test_format_time_finished_or_failed_returns_colored_date(state, start_end_walltime):
    start, end, walltime = start_end_walltime
    result = JobsPresenter._formatTime(state, start, end, walltime)
    color_code = JobsPresenter.ANSI_COLORS[state.color]
    formatted_date = end.strftime(CFG.date_formats.standard)

    assert formatted_date in result
    assert color_code in result
    assert result.endswith(JobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize(
    "state", [BatchState.HELD, BatchState.QUEUED, BatchState.WAITING, BatchState.MOVING]
)
def test_format_time_waiting_like_states_show_elapsed_duration(
    state, start_end_walltime
):
    start, end, walltime = start_end_walltime
    duration_str = format_duration_wdhhmmss(end - start)
    result = JobsPresenter._formatTime(state, start, end, walltime)
    color_code = JobsPresenter.ANSI_COLORS[state.color]

    assert duration_str in result
    assert color_code in result
    assert result.endswith(JobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("state", [BatchState.RUNNING, BatchState.EXITING])
def test_format_time_running_or_exiting_within_walltime(state, start_end_walltime):
    start, end, walltime = start_end_walltime  # 1 hour elapsed, 2-hour walltime
    run_duration_str = format_duration_wdhhmmss(end - start)
    walltime_str = format_duration_wdhhmmss(walltime)
    result = JobsPresenter._formatTime(state, start, end, walltime)

    # should use state's color (not strong warning)
    color_code = JobsPresenter.ANSI_COLORS[state.color]
    assert run_duration_str in result
    assert color_code in result
    assert f"/ {walltime_str}" in result
    assert result.endswith(JobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("state", [BatchState.RUNNING, BatchState.EXITING])
def test_format_time_running_or_exiting_exceeding_walltime_uses_strong_warning(
    state, start_end_walltime
):
    start, _, walltime = start_end_walltime
    end = start + timedelta(hours=3)  # exceeds walltime by 1 hour
    run_duration_str = format_duration_wdhhmmss(end - start)
    walltime_str = format_duration_wdhhmmss(walltime)
    result = JobsPresenter._formatTime(state, start, end, walltime)

    # should use strong warning color for run time
    warning_color_code = JobsPresenter.ANSI_COLORS[
        CFG.jobs_presenter.strong_warning_style
    ]
    assert run_duration_str in result
    assert warning_color_code in result
    assert f"/ {walltime_str}" in result
    assert result.endswith(JobsPresenter.ANSI_COLORS["reset"])


@pytest.fixture
def sample_pbs_dump():
    return """
Job Id: 123456.fake-cluster.example.com
    Job_Name = example_job_1
    Job_Owner = user1@EXAMPLE
    resources_used.cpupercent = 75
    resources_used.cput = 01:23:45
    resources_used.mem = 51200kb
    resources_used.ncpus = 4
    resources_used.vmem = 51200kb
    resources_used.walltime = 01:00:00
    job_state = R
    queue = gpu
    server = fake-cluster.example.com
    ctime = Sun Sep 21 00:00:00 2025
    mtime = Sun Sep 21 01:00:00 2025
    Resource_List.ncpus = 4
    Resource_List.ngpus = 1
    Resource_List.nodect = 1
    Resource_List.walltime = 02:00:00
    exec_host = nodeA/4*4
    exec_vnode = (nodeA:ncpus=4:ngpus=1:mem=4096mb)
    Output_Path = /fake/path/job_123456.log
    stime = Sun Sep 21 00:00:00 2025
    jobdir = /fake/home/user1

Job Id: 654321.fake-cluster.example.com
    Job_Name = example_job_2
    Job_Owner = user2@EXAMPLE
    resources_used.cpupercent = 150
    resources_used.cput = 02:34:56
    resources_used.mem = 102400kb
    resources_used.ncpus = 8
    resources_used.vmem = 102400kb
    resources_used.walltime = 02:00:00
    job_state = Q
    queue = batch
    server = fake-cluster.example.com
    ctime = Sun Sep 21 00:00:00 2025
    mtime = Sun Sep 21 01:00:00 2025
    Resource_List.ncpus = 8
    Resource_List.ngpus = 0
    Resource_List.nodect = 2
    Resource_List.walltime = 04:00:00
    exec_host = nodeB/8*8
    exec_vnode = (nodeB:ncpus=8:mem=8192mb)
    Output_Path = /fake/path/job_654321.log
    jobdir = /fake/home/user2
""".strip()


@pytest.fixture
def parsed_jobs(sample_pbs_dump):
    jobs = []
    for data, job_id in parse_multi_pbs_dump_to_dictionaries(sample_pbs_dump, "Job Id"):
        jobs.append(PBSJob.fromDict(job_id, data))
    return jobs


def test_dump_yaml_roundtrip(parsed_jobs):
    presenter = JobsPresenter(PBS, parsed_jobs, False, True)

    # capture stdout
    captured = io.StringIO()
    sys.stdout = captured
    try:
        presenter.dumpYaml()
    finally:
        sys.stdout = sys.__stdout__

    yaml_output = captured.getvalue().strip().split("\n\n")
    reloaded_jobs = []

    for doc in yaml_output:
        if not doc.strip():
            continue
        data = yaml.safe_load(doc)
        reloaded_jobs.append(PBSJob.fromDict(data["Job Id"], data))

    # check that the number of jobs matches
    assert len(reloaded_jobs) == len(parsed_jobs)

    # compare key fields
    for orig, loaded in zip(parsed_jobs, reloaded_jobs):
        assert orig.getId() == loaded.getId()
        assert orig.getName() == loaded.getName()
        assert orig.getUser() == loaded.getUser()
        assert orig.getQueue() == loaded.getQueue()
        assert orig.getWalltime() == loaded.getWalltime()
        assert orig.getNCPUs() == loaded.getNCPUs()
        assert orig.getNGPUs() == loaded.getNGPUs()
        assert orig.getState() == loaded.getState()
        assert orig.getInputDir() == loaded.getInputDir()


def test_create_jobs_info_panel_structure(parsed_jobs):
    presenter = JobsPresenter(PBS, parsed_jobs, False, True)
    panel_group = presenter.createJobsInfoPanel()

    assert isinstance(panel_group, Group)
    assert len(panel_group.renderables) == 3

    main_panel = panel_group.renderables[1]
    assert isinstance(main_panel, Panel)

    assert isinstance(main_panel.title, Text)
    assert "COLLECTED JOBS" in main_panel.title.plain

    content = main_panel.renderable
    assert isinstance(content, Group)
    assert len(content.renderables) >= 2

    jobs_table = content.renderables[0]
    assert isinstance(jobs_table, Text)
    assert all(
        JobsPresenter._shortenJobId(job.getId()) in jobs_table.plain
        for job in parsed_jobs
    )


@pytest.mark.parametrize("extra_flag,should_call", [(True, True), (False, False)])
def test_jobs_presenter_create_jobs_info_panel_insert_extra_info(
    extra_flag, should_call
):
    presenter = JobsPresenter.__new__(JobsPresenter)
    presenter._extra = extra_flag
    presenter._createBasicJobsTable = Mock(return_value="BASIC_TABLE")
    presenter._stats = Mock()
    presenter._stats.createStatsPanel.return_value = Mock()

    with patch.object(
        presenter, "_insertExtraInfo", return_value="UPDATED_TABLE"
    ) as mock_insert:
        presenter.createJobsInfoPanel(console=Mock(size=Mock(width=100)))

    assert mock_insert.called is should_call
    if should_call:
        mock_insert.assert_called_once_with("BASIC_TABLE")


@pytest.mark.parametrize(
    "job_name,expected,should_truncate",
    [
        # 1. shorter than limit
        ("short_name", "short_name", False),
        # 2. exactly at limit
        (
            "a" * CFG.jobs_presenter.max_job_name_length,
            "a" * CFG.jobs_presenter.max_job_name_length,
            False,
        ),
        # 3. exceeds limit
        ("a" * (CFG.jobs_presenter.max_job_name_length + 1), None, True),
        # 4. exceeds limit by a lot
        ("a" * (CFG.jobs_presenter.max_job_name_length + 15), None, True),
        # 5. empty string
        ("", "", False),
        # 6. whitespace only
        (" " * 5, " " * 5, False),
    ],
)
def test_jobs_presenter_shorten_job_name(job_name, expected, should_truncate):
    result = JobsPresenter._shortenJobName(job_name)

    if should_truncate:
        assert result.endswith("…")
        assert len(result) == CFG.jobs_presenter.max_job_name_length + 1
        assert result.startswith(job_name[: CFG.jobs_presenter.max_job_name_length])
    else:
        # must not be truncated
        assert result == expected
        assert "…" not in result


@pytest.mark.parametrize(
    "nodes,expected,should_truncate",
    [
        # 1. shorter than limit
        ("nodeA + nodeB + nodeC", "nodeA + nodeB + nodeC", False),
        # 2. exactly at limit
        (
            "n" * CFG.jobs_presenter.max_nodes_length,
            "n" * CFG.jobs_presenter.max_nodes_length,
            False,
        ),
        # 3. exceeds limit by one
        ("n" * (CFG.jobs_presenter.max_nodes_length + 1), None, True),
        # 4. exceeds limit by a lot
        ("x" * (CFG.jobs_presenter.max_nodes_length + 20), None, True),
        # 5. empty string
        ("", "", False),
        # 6. whitespace only
        (" " * 4, " " * 4, False),
    ],
)
def test_jobs_presenter_shorten_nodes(nodes, expected, should_truncate):
    result = JobsPresenter._shortenNodes(nodes)

    if should_truncate:
        assert result.endswith("…")
        assert len(result) == CFG.jobs_presenter.max_nodes_length + 1
        assert result.startswith(nodes[: CFG.jobs_presenter.max_nodes_length])
    else:
        assert result == expected
        assert "…" not in result


@pytest.mark.parametrize(
    "string,color,bold,expected_style",
    [
        ("hello", None, False, " "),
        ("hello", None, True, " bold"),
        ("hello", "red", False, "red "),
        ("hello", "green", True, "green bold"),
    ],
)
def test_jobs_statistics_color_text_variants(string, color, bold, expected_style):
    text_obj = JobsStatistics._colorText(string, color=color, bold=bold)
    assert isinstance(text_obj, Text)
    assert text_obj.plain == string
    assert text_obj.style == expected_style


def test_jobs_statistics_color_text_default_behavior():
    text_obj = JobsStatistics._colorText("test")
    assert isinstance(text_obj, Text)
    assert text_obj.plain == "test"
    assert text_obj.style == " "


@pytest.mark.parametrize("bold", [False, True])
def test_jobs_statistics_secondary_color_text_applies_correct_color_and_bold(bold):
    text_obj = JobsStatistics._secondaryColorText("example", bold=bold)
    assert isinstance(text_obj, Text)
    assert text_obj.plain == "example"
    expected_style = f"{CFG.jobs_presenter.secondary_style}{' bold' if bold else ' '}"
    assert text_obj.style == expected_style


def test_jobs_statistics_create_resources_stats_table_structure():
    stats = JobsStatistics(
        n_requested_cpus=16,
        n_requested_gpus=2,
        n_requested_nodes=4,
        n_allocated_cpus=8,
        n_allocated_gpus=1,
        n_allocated_nodes=3,
    )

    table = stats._createResourcesStatsTable()
    console = Console(record=True, width=100)
    console.print(table)
    output_lines = console.export_text().splitlines()

    header_line = output_lines[0]
    assert "CPUs" in header_line
    assert "GPUs" in header_line
    assert "Nodes" in header_line

    requested_line = next(line for line in output_lines if "Requested" in line)
    assert "16" in requested_line
    assert "2" in requested_line
    assert "4" in requested_line

    allocated_line = next(line for line in output_lines if "Allocated" in line)
    assert "8" in allocated_line
    assert "1" in allocated_line
    assert "3" in allocated_line

    # Unknown not displayed
    assert all("Unknown" not in line for line in output_lines)


def test_jobs_statistics_create_resources_stats_with_unknown_table_structure():
    stats = JobsStatistics(
        n_requested_cpus=16,
        n_requested_gpus=2,
        n_requested_nodes=4,
        n_allocated_cpus=8,
        n_allocated_gpus=1,
        n_allocated_nodes=3,
        n_unknown_cpus=9,
        n_unknown_gpus=0,
        n_unknown_nodes=5,
    )

    table = stats._createResourcesStatsTable()
    console = Console(record=True, width=100)
    console.print(table)
    output_lines = console.export_text().splitlines()

    header_line = output_lines[0]
    assert "CPUs" in header_line
    assert "GPUs" in header_line
    assert "Nodes" in header_line

    requested_line = next(line for line in output_lines if "Requested" in line)
    assert "16" in requested_line
    assert "2" in requested_line
    assert "4" in requested_line

    allocated_line = next(line for line in output_lines if "Allocated" in line)
    assert "8" in allocated_line
    assert "1" in allocated_line
    assert "3" in allocated_line

    unknown_line = next(line for line in output_lines if "Unknown" in line)
    assert "9" in unknown_line
    assert "0" in unknown_line
    assert "5" in unknown_line


def test_create_job_states_stats_no_jobs():
    stats = JobsStatistics()
    line = stats._createJobStatesStats()

    for state in BatchState:
        assert state.toCode() not in line.plain

    assert "Σ" in line.plain
    assert "0" in line.plain


def test_create_job_states_stats_some_jobs():
    stats = JobsStatistics()
    stats.n_jobs = {
        BatchState.RUNNING: 2,
        BatchState.QUEUED: 3,
        BatchState.FINISHED: 1,
    }
    line = stats._createJobStatesStats()

    present_states = {BatchState.RUNNING, BatchState.QUEUED, BatchState.FINISHED}
    for state in BatchState:
        if state in present_states:
            assert state.toCode() in line.plain
        else:
            assert state.toCode() not in line.plain

    assert "Σ" in line.plain
    assert "6" in line.plain


def test_create_job_states_stats_all_states_at_least_one_random():
    import random

    random.seed(42)  # fixed seed for reproducibility
    stats = JobsStatistics()

    stats.n_jobs = {state: random.randint(1, 10) for state in BatchState}

    line = stats._createJobStatesStats()

    for state in BatchState:
        assert state.toCode() in line.plain

    total_jobs = sum(stats.n_jobs.values())
    assert "Σ" in line.plain
    assert str(total_jobs) in line.plain


def test_add_job_queued_counts_requested():
    stats = JobsStatistics()
    stats.addJob(BatchState.QUEUED, cpus=4, gpus=1, nodes=2)

    assert stats.n_jobs[BatchState.QUEUED] == 1

    assert stats.n_requested_cpus == 4
    assert stats.n_requested_gpus == 1
    assert stats.n_requested_nodes == 2

    assert stats.n_allocated_cpus == 0
    assert stats.n_allocated_gpus == 0
    assert stats.n_allocated_nodes == 0

    assert stats.n_unknown_cpus == 0
    assert stats.n_unknown_gpus == 0
    assert stats.n_unknown_nodes == 0


def test_add_job_held_counts_requested():
    stats = JobsStatistics()
    stats.addJob(BatchState.HELD, cpus=2, gpus=0, nodes=1)

    assert stats.n_jobs[BatchState.HELD] == 1

    assert stats.n_requested_cpus == 2
    assert stats.n_requested_gpus == 0
    assert stats.n_requested_nodes == 1

    assert stats.n_allocated_cpus == 0
    assert stats.n_allocated_gpus == 0
    assert stats.n_allocated_nodes == 0

    assert stats.n_unknown_cpus == 0
    assert stats.n_unknown_gpus == 0
    assert stats.n_unknown_nodes == 0


def test_add_job_running_counts_allocated():
    stats = JobsStatistics()
    stats.addJob(BatchState.RUNNING, cpus=8, gpus=2, nodes=4)

    assert stats.n_jobs[BatchState.RUNNING] == 1

    assert stats.n_allocated_cpus == 8
    assert stats.n_allocated_gpus == 2
    assert stats.n_allocated_nodes == 4

    assert stats.n_requested_cpus == 0
    assert stats.n_requested_gpus == 0
    assert stats.n_requested_nodes == 0

    assert stats.n_unknown_cpus == 0
    assert stats.n_unknown_gpus == 0
    assert stats.n_unknown_nodes == 0


def test_add_job_exiting_counts_allocated():
    stats = JobsStatistics()
    stats.addJob(BatchState.EXITING, cpus=16, gpus=4, nodes=8)

    assert stats.n_jobs[BatchState.EXITING] == 1
    assert stats.n_allocated_cpus == 16
    assert stats.n_allocated_gpus == 4
    assert stats.n_allocated_nodes == 8

    assert stats.n_requested_cpus == 0
    assert stats.n_requested_gpus == 0
    assert stats.n_requested_nodes == 0

    assert stats.n_unknown_cpus == 0
    assert stats.n_unknown_gpus == 0
    assert stats.n_unknown_nodes == 0


def test_add_job_unknown_counts_unknown():
    stats = JobsStatistics()
    stats.addJob(BatchState.UNKNOWN, cpus=16, gpus=4, nodes=8)

    assert stats.n_jobs[BatchState.UNKNOWN] == 1
    assert stats.n_allocated_cpus == 0
    assert stats.n_allocated_gpus == 0
    assert stats.n_allocated_nodes == 0

    assert stats.n_requested_cpus == 0
    assert stats.n_requested_gpus == 0
    assert stats.n_requested_nodes == 0

    assert stats.n_unknown_cpus == 16
    assert stats.n_unknown_gpus == 4
    assert stats.n_unknown_nodes == 8


@pytest.mark.parametrize(
    "state",
    [
        BatchState.FINISHED,
        BatchState.FAILED,
        BatchState.SUSPENDED,
        BatchState.MOVING,
        BatchState.WAITING,
    ],
)
def test_add_job_other_states_not_counted(state):
    stats = JobsStatistics()
    stats.addJob(state, cpus=10, gpus=5, nodes=3)

    assert stats.n_jobs[state] == 1

    assert stats.n_requested_cpus == 0
    assert stats.n_requested_gpus == 0
    assert stats.n_requested_nodes == 0
    assert stats.n_allocated_cpus == 0
    assert stats.n_allocated_gpus == 0
    assert stats.n_allocated_nodes == 0
    assert stats.n_unknown_cpus == 0
    assert stats.n_unknown_gpus == 0
    assert stats.n_unknown_nodes == 0


def test_add_job_multiple_same_state_accumulates():
    stats = JobsStatistics()
    stats.addJob(BatchState.QUEUED, cpus=2, gpus=1, nodes=1)
    stats.addJob(BatchState.QUEUED, cpus=3, gpus=0, nodes=2)

    assert stats.n_jobs[BatchState.QUEUED] == 2
    assert stats.n_requested_cpus == 5
    assert stats.n_requested_gpus == 1
    assert stats.n_requested_nodes == 3


def test_add_job_mixed_states_accumulates_correctly():
    stats = JobsStatistics()
    stats.addJob(BatchState.QUEUED, cpus=2, gpus=1, nodes=1)
    stats.addJob(BatchState.RUNNING, cpus=4, gpus=2, nodes=2)
    stats.addJob(BatchState.HELD, cpus=1, gpus=0, nodes=1)
    stats.addJob(BatchState.EXITING, cpus=3, gpus=1, nodes=1)

    assert stats.n_jobs[BatchState.QUEUED] == 1
    assert stats.n_jobs[BatchState.RUNNING] == 1
    assert stats.n_jobs[BatchState.HELD] == 1
    assert stats.n_jobs[BatchState.EXITING] == 1

    assert stats.n_requested_cpus == 3
    assert stats.n_requested_gpus == 1
    assert stats.n_requested_nodes == 2

    assert stats.n_allocated_cpus == 7
    assert stats.n_allocated_gpus == 3
    assert stats.n_allocated_nodes == 3


@pytest.mark.parametrize(
    "input_machine,input_dir,expected_machine,expected_dir",
    [
        # both have valid info
        ("machine1", "/path/dir", True, True),
        # input machine missing, dir valid
        (None, "/path/dir", False, True),
        # input machine valid, dir missing
        ("machine2", None, True, False),
        # both missing
        (None, None, False, False),
    ],
)
def test_jobs_presenter_insert_extra_info_various_combinations(
    input_machine, input_dir, expected_machine, expected_dir
):
    job = Mock()
    job.getInputMachine.return_value = input_machine
    job.getInputDir.return_value = input_dir

    presenter = JobsPresenter.__new__(JobsPresenter)
    presenter._jobs = [job]

    table = "HEADER\nROW1"

    result = presenter._insertExtraInfo(table)

    assert "HEADER" in result
    assert "ROW1" in result

    assert (">   Input machine:" in result) == expected_machine
    assert (">   Input directory:" in result) == expected_dir


def test_jobs_presenter_insert_extra_info_multiple_jobs():
    job1 = Mock()
    job1.getInputMachine.return_value = "machineA"
    job1.getInputDir.return_value = "/dirA"

    job2 = Mock()
    job2.getInputMachine.return_value = "machineB"
    job2.getInputDir.return_value = "/dirB"

    presenter = JobsPresenter.__new__(JobsPresenter)
    presenter._jobs = [job1, job2]

    table = "HEADER\nROW1\nROW2"
    result = presenter._insertExtraInfo(table)

    assert "machineA" in result
    assert "machineB" in result
    assert "/dirA" in result
    assert "/dirB" in result


def test_jobs_presenter_insert_extra_info_preserves_header_and_spacing():
    job = Mock()
    job.getInputMachine.return_value = "machineX"
    job.getInputDir.return_value = "/inputX"

    presenter = JobsPresenter.__new__(JobsPresenter)
    presenter._jobs = [job]

    table = "HEADER\nROW1"
    result = presenter._insertExtraInfo(table)

    assert result.startswith("HEADER\n")
    assert result.strip().endswith("")


def test_jobs_presenter_insert_extra_info_uses_cfg_style():
    job = Mock()
    job.getInputMachine.return_value = "machineZ"
    job.getInputDir.return_value = "/inputZ"

    presenter = JobsPresenter.__new__(JobsPresenter)
    presenter._jobs = [job]

    result = presenter._insertExtraInfo("HEADER\nROW1")

    assert JobsPresenter.ANSI_COLORS[CFG.jobs_presenter.extra_info_style] in result


def test_format_exit_code_returns_empty_string_when_exit_code_is_none():
    job = Mock()
    job.getExitCode.return_value = None

    result = JobsPresenter._formatExitCode(job, BatchState.RUNNING)

    assert result == ""


def test_format_exit_code_returns_main_colored_string_for_finished_state():
    job = Mock()
    job.getExitCode.return_value = 0
    mock_cfg = Mock()
    mock_cfg.jobs_presenter.main_style = "cyan"

    with patch("qq_lib.jobs.presenter.CFG", mock_cfg):
        result = JobsPresenter._formatExitCode(job, BatchState.FINISHED)

    assert result == "\033[36m0\033[0m"


def test_format_exit_code_returns_main_colored_string_for_finished_state_with_nonzero_exit_code():
    job = Mock()
    job.getExitCode.return_value = 1
    mock_cfg = Mock()
    mock_cfg.jobs_presenter.main_style = "green"

    with patch("qq_lib.jobs.presenter.CFG", mock_cfg):
        result = JobsPresenter._formatExitCode(job, BatchState.FINISHED)

    assert result == "\033[32m1\033[0m"


def test_format_exit_code_returns_warning_colored_string_for_failed_state():
    job = Mock()
    job.getExitCode.return_value = 1
    mock_cfg = Mock()
    mock_cfg.jobs_presenter.strong_warning_style = "red"

    with patch("qq_lib.jobs.presenter.CFG", mock_cfg):
        result = JobsPresenter._formatExitCode(job, BatchState.FAILED)

    assert result == "\033[31m1\033[0m"


def test_format_exit_code_returns_warning_colored_string_for_failed_state_with_zero_exit_code():
    job = Mock()
    job.getExitCode.return_value = 0
    mock_cfg = Mock()
    mock_cfg.jobs_presenter.strong_warning_style = "bright_red"

    with patch("qq_lib.jobs.presenter.CFG", mock_cfg):
        result = JobsPresenter._formatExitCode(job, BatchState.FAILED)

    assert result == "\033[91m0\033[0m"


@pytest.mark.parametrize(
    "state", [BatchState.QUEUED, BatchState.RUNNING, BatchState.HELD]
)
def test_format_exit_code_returns_empty_string_for_various_states(state):
    job = Mock()
    job.getExitCode.return_value = 0

    result = JobsPresenter._formatExitCode(job, state)

    assert result == ""


def test_format_headers_returns_formatted_list():
    presenter = JobsPresenter(PBS, [], False, False)
    mock_cfg = Mock()
    mock_cfg.jobs_presenter.headers_style = "cyan"

    with patch("qq_lib.jobs.presenter.CFG", mock_cfg):
        result = presenter._formatHeaders(["S", "Job ID", "User"])

    assert result == [
        "\033[1m\033[36mS\033[0m",
        "\033[1m\033[36mJob ID\033[0m",
        "\033[1m\033[36mUser\033[0m",
    ]


def test_format_headers_returns_empty_list_for_empty_input():
    presenter = JobsPresenter(PBS, [], False, False)
    mock_cfg = Mock()
    mock_cfg.jobs_presenter.headers_style = "cyan"

    with patch("qq_lib.jobs.presenter.CFG", mock_cfg):
        result = presenter._formatHeaders([])

    assert result == []


@pytest.mark.parametrize(
    "state",
    [
        BatchState.QUEUED,
        BatchState.HELD,
        BatchState.WAITING,
    ],
)
def test_get_job_times_returns_submission_time_for_non_started_states(state):
    job = Mock()
    submission_time = datetime(2025, 1, 1, 10, 0, 0)
    job.getSubmissionTime.return_value = submission_time
    now = datetime(2025, 1, 1, 11, 0, 0)

    with patch("qq_lib.jobs.presenter.datetime") as mock_datetime:
        mock_datetime.now.return_value = now
        start_time, end_time = JobsPresenter._getJobTimes(job, state)

    assert start_time == submission_time
    assert end_time == now


def test_get_job_times_returns_start_time_for_running_state():
    job = Mock()
    start_time = datetime(2025, 1, 1, 10, 30, 0)
    job.getStartTime.return_value = start_time
    now = datetime(2025, 1, 1, 11, 0, 0)

    with patch("qq_lib.jobs.presenter.datetime") as mock_datetime:
        mock_datetime.now.return_value = now
        result_start, result_end = JobsPresenter._getJobTimes(job, BatchState.RUNNING)

    assert result_start == start_time
    assert result_end == now


@pytest.mark.parametrize(
    "state",
    [
        BatchState.FINISHED,
        BatchState.FAILED,
    ],
)
def test_get_job_times_returns_completion_time_for_completed_states(state):
    job = Mock()
    start_time = datetime(2025, 1, 1, 10, 0, 0)
    completion_time = datetime(2025, 1, 1, 11, 0, 0)
    job.getStartTime.return_value = start_time
    job.getCompletionTime.return_value = completion_time

    result_start, result_end = JobsPresenter._getJobTimes(job, state)

    assert result_start == start_time
    assert result_end == completion_time


def test_get_job_times_falls_back_to_modification_time_when_completion_time_is_none():
    job = Mock()
    start_time = datetime(2025, 1, 1, 10, 0, 0)
    modification_time = datetime(2025, 1, 1, 11, 0, 0)
    job.getStartTime.return_value = start_time
    job.getCompletionTime.return_value = None
    job.getModificationTime.return_value = modification_time

    result_start, result_end = JobsPresenter._getJobTimes(job, BatchState.FINISHED)

    assert result_start == start_time
    assert result_end == modification_time


def test_get_visible_headers_returns_headers_in_batch_system_config():
    batch_system = Mock()
    batch_system.jobsPresenterColumnsToShow.return_value = [
        "S",
        "Job ID",
        "User",
        "Queue",
    ]

    presenter = JobsPresenter.__new__(JobsPresenter)
    presenter._batch_system = batch_system
    presenter._all = False

    result = presenter._getVisibleHeaders()

    assert result == ["S", "Job ID", "User", "Queue"]


def test_get_visible_headers_includes_exit_when_all_is_true():
    batch_system = Mock()
    batch_system.jobsPresenterColumnsToShow.return_value = ["S", "Job ID", "Exit"]

    presenter = JobsPresenter.__new__(JobsPresenter)
    presenter._batch_system = batch_system
    presenter._all = True

    result = presenter._getVisibleHeaders()

    assert "Exit" in result


def test_get_visible_headers_excludes_exit_when_all_is_false():
    batch_system = Mock()
    batch_system.jobsPresenterColumnsToShow.return_value = ["S", "Job ID", "Exit"]
    presenter = JobsPresenter.__new__(JobsPresenter)
    presenter._batch_system = batch_system
    presenter._all = False

    result = presenter._getVisibleHeaders()

    assert "Exit" not in result


def test_create_job_row_returns_row_with_requested_headers():
    job = Mock()
    job.getState.return_value = Mock(toCode=Mock(return_value="R"), color="green")
    job.getId.return_value = "12345"
    job.getUser.return_value = "user1"
    job.getName.return_value = "job_name"
    job.getQueue.return_value = "default"
    job.getNCPUs.return_value = 4
    job.getNGPUs.return_value = 2
    job.getNNodes.return_value = 1

    presenter = JobsPresenter.__new__(JobsPresenter)
    presenter._all = False
    presenter._stats = Mock()

    with (
        patch.object(
            JobsPresenter, "_getJobTimes", return_value=(datetime.now(), datetime.now())
        ),
        patch.object(JobsPresenter, "_shortenJobId", return_value="12345"),
        patch.object(JobsPresenter, "_shortenJobName", return_value="job_name"),
        patch.object(JobsPresenter, "_formatTime", return_value="time"),
        patch.object(JobsPresenter, "_formatNodesOrComment", return_value="node"),
        patch.object(JobsPresenter, "_formatUtilCPU", return_value="cpu"),
        patch.object(JobsPresenter, "_formatUtilMem", return_value="mem"),
    ):
        result = presenter._createJobRow(job, ["S", "Job ID", "NCPUs"])

    assert len(result) == 3
    assert "R" in result[0]
    assert "12345" in result[1]
    assert "4" in result[2]


def test_create_job_row_calls_add_job_on_stats():
    job = Mock()
    state = Mock(toCode=Mock(return_value="R"), color="green")
    job.getState.return_value = state
    job.getNCPUs.return_value = 4
    job.getNGPUs.return_value = 2
    job.getNNodes.return_value = 1

    presenter = JobsPresenter.__new__(JobsPresenter)
    presenter._all = False
    presenter._stats = Mock()
    mock_cfg = Mock()
    mock_cfg.jobs_presenter.main_style = "cyan"

    with (
        patch.object(
            JobsPresenter, "_getJobTimes", return_value=(datetime.now(), datetime.now())
        ),
        patch.object(JobsPresenter, "_shortenJobId", return_value="id"),
        patch.object(JobsPresenter, "_shortenJobName", return_value="name"),
        patch.object(JobsPresenter, "_formatTime", return_value="time"),
        patch.object(JobsPresenter, "_formatNodesOrComment", return_value="node"),
        patch.object(JobsPresenter, "_formatUtilCPU", return_value="cpu"),
        patch.object(JobsPresenter, "_formatUtilMem", return_value="mem"),
    ):
        presenter._createJobRow(job, ["S"])

    presenter._stats.addJob.assert_called_once_with(state, 4, 2, 1)


def test_create_basic_jobs_table_creates_row_for_each_job_with_headers():
    job1 = Mock()
    job2 = Mock()
    job3 = Mock()
    presenter = JobsPresenter.__new__(JobsPresenter)
    presenter._jobs = [job1, job2, job3]

    with (
        patch.object(
            presenter, "_getVisibleHeaders", return_value=["S", "Job ID", "User"]
        ),
        patch.object(
            presenter,
            "_formatHeaders",
            return_value=["fmt_S", "fmt_Job_ID", "fmt_User"],
        ),
        patch.object(
            presenter,
            "_createJobRow",
            side_effect=[
                ["row1_col1", "row1_col2", "row1_col3"],
                ["row2_col1", "row2_col2", "row2_col3"],
                ["row3_col1", "row3_col2", "row3_col3"],
            ],
        ) as mock_create_row,
        patch("qq_lib.jobs.presenter.tabulate", return_value="output") as mock_tabulate,
    ):
        result = presenter._createBasicJobsTable()

    assert mock_create_row.call_count == 3
    mock_create_row.assert_any_call(job1, ["S", "Job ID", "User"])
    mock_create_row.assert_any_call(job2, ["S", "Job ID", "User"])
    mock_create_row.assert_any_call(job3, ["S", "Job ID", "User"])
    mock_tabulate.assert_called_once_with(
        [
            ["row1_col1", "row1_col2", "row1_col3"],
            ["row2_col1", "row2_col2", "row2_col3"],
            ["row3_col1", "row3_col2", "row3_col3"],
        ],
        headers=["fmt_S", "fmt_Job_ID", "fmt_User"],
        tablefmt=JobsPresenter.COMPACT_TABLE,
        stralign="center",
        numalign="center",
    )
    assert result == "output"
