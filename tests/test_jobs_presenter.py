# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import io
import sys
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
import yaml
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text

from qq_lib.batch.pbs import PBSJobInfo
from qq_lib.core.common import format_duration_wdhhmmss
from qq_lib.core.constants import (
    DATE_FORMAT,
    JOBS_PRESENTER_MAIN_COLOR,
    JOBS_PRESENTER_MAX_JOB_NAME_LENGTH,
    JOBS_PRESENTER_MILD_WARNING_COLOR,
    JOBS_PRESENTER_SECONDARY_COLOR,
    JOBS_PRESENTER_STRONG_WARNING_COLOR,
)
from qq_lib.jobs.presenter import QQJobsPresenter, QQJobsStatistics
from qq_lib.properties.states import BatchState


@pytest.mark.parametrize(
    "string,color,bold,expected_prefix",
    [
        ("test", None, False, ""),  # no color, no bold
        ("test", "red", False, QQJobsPresenter.ANSI_COLORS["red"]),
        ("test", None, True, QQJobsPresenter.ANSI_COLORS["bold"]),
        (
            "test",
            "green",
            True,
            QQJobsPresenter.ANSI_COLORS["bold"] + QQJobsPresenter.ANSI_COLORS["green"],
        ),
    ],
)
def test_color_applies_correct_ansi(string, color, bold, expected_prefix):
    result = QQJobsPresenter._color(string, color=color, bold=bold)
    reset = QQJobsPresenter.ANSI_COLORS["reset"] if color or bold else ""
    assert result == f"{expected_prefix}{string}{reset}"


def test_main_color_applies_main_color():
    text = "text"
    result = QQJobsPresenter._mainColor(text)
    expected = f"{QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_MAIN_COLOR]}text{QQJobsPresenter.ANSI_COLORS['reset']}"
    assert result == expected


def test_main_color_applies_main_color_and_bold():
    text = "text"
    result = QQJobsPresenter._mainColor(text, bold=True)
    expected = f"{QQJobsPresenter.ANSI_COLORS['bold']}{QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_MAIN_COLOR]}text{QQJobsPresenter.ANSI_COLORS['reset']}"
    assert result == expected


def test_secondary_color_applies_secondary_color():
    text = "text"
    result = QQJobsPresenter._secondaryColor(text)
    expected = f"{QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_SECONDARY_COLOR]}text{QQJobsPresenter.ANSI_COLORS['reset']}"
    assert result == expected


def test_secondary_color_applies_secondary_color_and_bold():
    text = "text"
    result = QQJobsPresenter._secondaryColor(text, bold=True)
    expected = f"{QQJobsPresenter.ANSI_COLORS['bold']}{QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_SECONDARY_COLOR]}text{QQJobsPresenter.ANSI_COLORS['reset']}"
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
    assert QQJobsPresenter._shortenJobId(input_id) == expected


@pytest.fixture
def mock_job():
    return PBSJobInfo.__new__(PBSJobInfo)


def test_format_nodes_or_comment_returns_single_node(mock_job):
    with (
        patch.object(mock_job, "getShortNodes", return_value=["node1"]),
        patch.object(mock_job, "getEstimated", return_value=None),
    ):
        result = QQJobsPresenter._formatNodesOrComment(BatchState.RUNNING, mock_job)
        expected = QQJobsPresenter._mainColor("node1")
        assert result == expected


def test_format_nodes_or_comment_returns_nodes(mock_job):
    with (
        patch.object(mock_job, "getShortNodes", return_value=["node1", "node2"]),
        patch.object(mock_job, "getEstimated", return_value=None),
    ):
        result = QQJobsPresenter._formatNodesOrComment(BatchState.RUNNING, mock_job)
        expected = QQJobsPresenter._mainColor("node1 + node2")
        assert result == expected


@pytest.mark.parametrize("state", [BatchState.FINISHED, BatchState.FAILED])
def test_format_nodes_or_comment_finished_or_failed_no_nodes(mock_job, state):
    with (
        patch.object(mock_job, "getShortNodes", return_value=[]),
        patch.object(mock_job, "getEstimated", return_value=None),
    ):
        result = QQJobsPresenter._formatNodesOrComment(state, mock_job)
        assert result == ""


@pytest.mark.parametrize("state", [BatchState.FINISHED, BatchState.FAILED])
def test_format_nodes_or_comment_finished_or_failed_single_node(mock_job, state):
    with (
        patch.object(mock_job, "getShortNodes", return_value=["node1"]),
        patch.object(mock_job, "getEstimated", return_value=None),
    ):
        result = QQJobsPresenter._formatNodesOrComment(state, mock_job)
        expected = QQJobsPresenter._mainColor("node1")
        assert result == expected


def test_format_nodes_or_comment_returns_estimated(mock_job):
    now = datetime.now()
    estimated_time = now + timedelta(hours=2)
    desc = "node01"

    with (
        patch.object(mock_job, "getShortNodes", return_value=[]),
        patch.object(mock_job, "getEstimated", return_value=(estimated_time, desc)),
    ):
        result = QQJobsPresenter._formatNodesOrComment(BatchState.QUEUED, mock_job)

        assert QQJobsPresenter.ANSI_COLORS[BatchState.QUEUED.color] in result
        assert desc in result
        duration_str = format_duration_wdhhmmss(estimated_time - datetime.now()).rsplit(
            ":", 1
        )[0]
        assert duration_str in result


def test_format_nodes_or_comment_returns_empty_when_no_info(mock_job):
    with (
        patch.object(mock_job, "getShortNodes", return_value=[]),
        patch.object(mock_job, "getEstimated", return_value=None),
    ):
        result = QQJobsPresenter._formatNodesOrComment(BatchState.QUEUED, mock_job)
        assert result == ""


def test_format_exit_code_none_returns_empty():
    result = QQJobsPresenter._formatExitCode(None)
    assert result == ""


def test_format_exit_code_zero_returns_main_color():
    result = QQJobsPresenter._formatExitCode(0)
    expected_color = QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_STRONG_WARNING_COLOR]
    main_colored = QQJobsPresenter._mainColor("0")

    assert result == main_colored
    assert "0" in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])
    # warning color should NOT be included
    assert expected_color not in result


@pytest.mark.parametrize("exit_code", [1, 42, 255, -1])
def test_format_exit_code_nonzero_returns_warning_color(exit_code):
    result = QQJobsPresenter._formatExitCode(exit_code)
    color_code = QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_STRONG_WARNING_COLOR]

    assert str(exit_code) in result
    assert color_code in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


def test_format_util_cpu_none_returns_empty():
    assert QQJobsPresenter._formatUtilCPU(None) == ""


@pytest.mark.parametrize("util", [101, 150, 300])
def test_format_util_cpu_above_100_uses_strong_warning(util):
    result = QQJobsPresenter._formatUtilCPU(util)
    color_code = QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_STRONG_WARNING_COLOR]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("util", [80, 85, 99, 100])
def test_format_util_cpu_80_to_100_uses_main_color(util):
    result = QQJobsPresenter._formatUtilCPU(util)
    color_code = QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_MAIN_COLOR]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("util", [60, 61, 79])
def test_format_util_cpu_60_to_79_uses_mild_warning(util):
    result = QQJobsPresenter._formatUtilCPU(util)
    color_code = QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_MILD_WARNING_COLOR]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("util", [0, 10, 59])
def test_format_util_cpu_below_60_uses_strong_warning(util):
    result = QQJobsPresenter._formatUtilCPU(util)
    color_code = QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_STRONG_WARNING_COLOR]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


def test_format_util_mem_none_returns_empty():
    assert QQJobsPresenter._formatUtilMem(None) == ""


@pytest.mark.parametrize("util", [0, 50, 89])
def test_format_util_mem_below_90_uses_main_color(util):
    result = QQJobsPresenter._formatUtilMem(util)
    color_code = QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_MAIN_COLOR]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("util", [90, 95, 99])
def test_format_util_mem_90_to_99_uses_mild_warning(util):
    result = QQJobsPresenter._formatUtilMem(util)
    color_code = QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_MILD_WARNING_COLOR]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("util", [100, 110, 150])
def test_format_util_mem_100_or_more_uses_strong_warning(util):
    result = QQJobsPresenter._formatUtilMem(util)
    color_code = QQJobsPresenter.ANSI_COLORS[JOBS_PRESENTER_STRONG_WARNING_COLOR]
    assert str(util) in result
    assert color_code in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


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
    result = QQJobsPresenter._formatTime(state, start, end, walltime)
    assert result == ""


@pytest.mark.parametrize("state", [BatchState.FINISHED, BatchState.FAILED])
def test_format_time_finished_or_failed_returns_colored_date(state, start_end_walltime):
    start, end, walltime = start_end_walltime
    result = QQJobsPresenter._formatTime(state, start, end, walltime)
    color_code = QQJobsPresenter.ANSI_COLORS[state.color]
    formatted_date = end.strftime(DATE_FORMAT)

    assert formatted_date in result
    assert color_code in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize(
    "state", [BatchState.HELD, BatchState.QUEUED, BatchState.WAITING, BatchState.MOVING]
)
def test_format_time_waiting_like_states_show_elapsed_duration(
    state, start_end_walltime
):
    start, end, walltime = start_end_walltime
    duration_str = format_duration_wdhhmmss(end - start)
    result = QQJobsPresenter._formatTime(state, start, end, walltime)
    color_code = QQJobsPresenter.ANSI_COLORS[state.color]

    assert duration_str in result
    assert color_code in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("state", [BatchState.RUNNING, BatchState.EXITING])
def test_format_time_running_or_exiting_within_walltime(state, start_end_walltime):
    start, end, walltime = start_end_walltime  # 1 hour elapsed, 2-hour walltime
    run_duration_str = format_duration_wdhhmmss(end - start)
    walltime_str = format_duration_wdhhmmss(walltime)
    result = QQJobsPresenter._formatTime(state, start, end, walltime)

    # should use state's color (not strong warning)
    color_code = QQJobsPresenter.ANSI_COLORS[state.color]
    assert run_duration_str in result
    assert color_code in result
    assert f"/ {walltime_str}" in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


@pytest.mark.parametrize("state", [BatchState.RUNNING, BatchState.EXITING])
def test_format_time_running_or_exiting_exceeding_walltime_uses_strong_warning(
    state, start_end_walltime
):
    start, _, walltime = start_end_walltime
    end = start + timedelta(hours=3)  # exceeds walltime by 1 hour
    run_duration_str = format_duration_wdhhmmss(end - start)
    walltime_str = format_duration_wdhhmmss(walltime)
    result = QQJobsPresenter._formatTime(state, start, end, walltime)

    # should use strong warning color for run time
    warning_color_code = QQJobsPresenter.ANSI_COLORS[
        JOBS_PRESENTER_STRONG_WARNING_COLOR
    ]
    assert run_duration_str in result
    assert warning_color_code in result
    assert f"/ {walltime_str}" in result
    assert result.endswith(QQJobsPresenter.ANSI_COLORS["reset"])


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
    for data, job_id in PBSJobInfo._parseMultiPBSDumpToDictionaries(sample_pbs_dump):
        jobs.append(PBSJobInfo.fromDict(job_id, data))
    return jobs


def test_create_basic_jobs_table_contains_all_headers_and_jobs(parsed_jobs):
    presenter = QQJobsPresenter(parsed_jobs)

    result = presenter._createBasicJobsTable()
    expected_headers = [
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

    for header in expected_headers:
        assert header in result

    for job in parsed_jobs:
        short_id = QQJobsPresenter._shortenJobId(job.getId())
        assert short_id in result

    for job in parsed_jobs:
        assert job.getName() in result
        assert job.getUser() in result

    count_1 = result.count(parsed_jobs[0].getName())
    count_2 = result.count(parsed_jobs[1].getName())
    assert count_1 == 1
    assert count_2 == 1

    assert "gpu" in result
    assert "batch" in result

    assert any(str(job.getNCPUs()) in result for job in parsed_jobs)
    assert any(str(job.getNGPUs()) in result for job in parsed_jobs)

    # ANSI colors
    assert "\033[" in result


def test_dump_yaml_roundtrip(parsed_jobs):
    presenter = QQJobsPresenter(parsed_jobs)

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
        reloaded_jobs.append(PBSJobInfo.fromDict(data["Job Id"], data))

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
    presenter = QQJobsPresenter(parsed_jobs)
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
        QQJobsPresenter._shortenJobId(job.getId()) in jobs_table.plain
        for job in parsed_jobs
    )


@pytest.mark.parametrize(
    "job_name,expected,should_truncate",
    [
        # 1. shorter than limit
        ("short_name", "short_name", False),
        # 2. exactly at limit
        (
            "a" * JOBS_PRESENTER_MAX_JOB_NAME_LENGTH,
            "a" * JOBS_PRESENTER_MAX_JOB_NAME_LENGTH,
            False,
        ),
        # 3. exceeds limit
        ("a" * (JOBS_PRESENTER_MAX_JOB_NAME_LENGTH + 1), None, True),
        # 4. exceeds limit by a lot
        ("a" * (JOBS_PRESENTER_MAX_JOB_NAME_LENGTH + 15), None, True),
        # 5. empty string
        ("", "", False),
        # 6. whitespace only
        (" " * 5, " " * 5, False),
    ],
)
def test_jobs_presenter_shorten_job_name_behavior(job_name, expected, should_truncate):
    result = QQJobsPresenter._shortenJobName(job_name)

    if should_truncate:
        # must end with ellipsis
        assert result.endswith("…")
        # should have correct total length (limit + ellipsis)
        assert len(result) == JOBS_PRESENTER_MAX_JOB_NAME_LENGTH + 1
        # prefix should match the original start
        assert result.startswith(job_name[:JOBS_PRESENTER_MAX_JOB_NAME_LENGTH])
    else:
        # must not be truncated
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
    text_obj = QQJobsStatistics._colorText(string, color=color, bold=bold)
    assert isinstance(text_obj, Text)
    assert text_obj.plain == string
    assert text_obj.style == expected_style


def test_jobs_statistics_color_text_default_behavior():
    text_obj = QQJobsStatistics._colorText("test")
    assert isinstance(text_obj, Text)
    assert text_obj.plain == "test"
    assert text_obj.style == " "


@pytest.mark.parametrize("bold", [False, True])
def test_jobs_statistics_secondary_color_text_applies_correct_color_and_bold(bold):
    text_obj = QQJobsStatistics._secondaryColorText("example", bold=bold)
    assert isinstance(text_obj, Text)
    assert text_obj.plain == "example"
    expected_style = f"{JOBS_PRESENTER_SECONDARY_COLOR}{' bold' if bold else ' '}"
    assert text_obj.style == expected_style


def test_jobs_statistics_create_resources_stats_table_structure():
    stats = QQJobsStatistics(
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
    stats = QQJobsStatistics(
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
    stats = QQJobsStatistics()
    line = stats._createJobStatesStats()

    for state in BatchState:
        assert state.toCode() not in line.plain

    assert "Σ" in line.plain
    assert "0" in line.plain


def test_create_job_states_stats_some_jobs():
    stats = QQJobsStatistics()
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
    stats = QQJobsStatistics()

    stats.n_jobs = {state: random.randint(1, 10) for state in BatchState}

    line = stats._createJobStatesStats()

    for state in BatchState:
        assert state.toCode() in line.plain

    total_jobs = sum(stats.n_jobs.values())
    assert "Σ" in line.plain
    assert str(total_jobs) in line.plain


def test_add_job_queued_counts_requested():
    stats = QQJobsStatistics()
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
    stats = QQJobsStatistics()
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
    stats = QQJobsStatistics()
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
    stats = QQJobsStatistics()
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
    stats = QQJobsStatistics()
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
    stats = QQJobsStatistics()
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
    stats = QQJobsStatistics()
    stats.addJob(BatchState.QUEUED, cpus=2, gpus=1, nodes=1)
    stats.addJob(BatchState.QUEUED, cpus=3, gpus=0, nodes=2)

    assert stats.n_jobs[BatchState.QUEUED] == 2
    assert stats.n_requested_cpus == 5
    assert stats.n_requested_gpus == 1
    assert stats.n_requested_nodes == 3


def test_add_job_mixed_states_accumulates_correctly():
    stats = QQJobsStatistics()
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
