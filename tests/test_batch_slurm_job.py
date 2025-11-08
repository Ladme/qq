# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from qq_lib.batch.slurm.job import SlurmJob
from qq_lib.core.error import QQError
from qq_lib.properties.size import Size
from qq_lib.properties.states import BatchState


def test_slurm_job_init_calls_update_and_sets_attributes():
    with patch.object(SlurmJob, "update") as mock_update:
        job = SlurmJob("12345")

    assert job._job_id == "12345"
    assert isinstance(job._info, dict)
    mock_update.assert_called_once()


def test_slurm_job_is_empty_true_when_info_empty():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {}
    assert job.isEmpty() is True


def test_slurm_job_is_empty_false_when_info_not_empty():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"key": "value"}
    assert job.isEmpty() is False


def test_slurm_job_get_id_returns_job_id():
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "abc123"
    assert job.getId() == "abc123"


def test_slurm_job_get_account_returns_value_when_present():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"Account": "fake-account"}
    assert job.getAccount() == "fake-account"


def test_slurm_job_get_account_returns_none_when_missing():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {}
    assert job.getAccount() is None


@patch("qq_lib.batch.slurm.job.subprocess.run")
@patch("qq_lib.batch.slurm.job.parse_slurm_dump_to_dictionary")
def test_slurm_job_update_scontrol_success(mock_parse, mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "output"
    mock_run.return_value = mock_result
    mock_parse.return_value = {"JobId": "123"}

    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "123"
    job.update()

    mock_run.assert_called_once()
    mock_parse.assert_called_once_with("output")
    assert job._info == {"JobId": "123"}


@patch("qq_lib.batch.slurm.job.subprocess.run")
@patch("qq_lib.batch.slurm.job.SlurmJob.fromSacctString")
def test_slurm_job_update_scontrol_fail_sacct_success(mock_from_sacct, mock_run):
    first = MagicMock(returncode=1, stderr="fail1")
    second = MagicMock(returncode=0, stdout="success2")
    mock_run.side_effect = [first, second]
    mock_from_sacct.return_value = MagicMock(_info={"parsed": "data"})

    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "456"
    job.update()

    assert mock_run.call_count == 2
    mock_from_sacct.assert_called_once()
    assert job._info == {"parsed": "data"}


@patch("qq_lib.batch.slurm.job.subprocess.run")
def test_slurm_job_update_scontrol_and_sacct_fail(mock_run):
    first = MagicMock(returncode=1, stderr="fail1")
    second = MagicMock(returncode=1, stderr="fail2")
    mock_run.side_effect = [first, second]

    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "789"
    job.update()

    assert mock_run.call_count == 2
    assert job._info == {}


def test_slurm_job_get_state_returns_unknown_when_no_jobstate():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {}
    assert job.getState() == BatchState.UNKNOWN


@pytest.mark.parametrize(
    "raw_state,expected_state",
    [
        ("BOOT_FAIL", BatchState.FAILED),
        ("CANCELLED", BatchState.FAILED),
        ("COMPLETED", BatchState.FINISHED),
        ("DEADLINE", BatchState.FAILED),
        ("FAILED", BatchState.FAILED),
        ("NODE_FAIL", BatchState.FAILED),
        ("OUT_OF_MEMORY", BatchState.FAILED),
        ("PENDING", BatchState.QUEUED),
        ("PREEMPTED", BatchState.SUSPENDED),
        ("RUNNING", BatchState.RUNNING),
        ("SUSPENDED", BatchState.SUSPENDED),
        ("TIMEOUT", BatchState.FAILED),
    ],
)
def test_slurm_job_get_state_returns_converted_value_for_all_states(
    raw_state, expected_state
):
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"JobState": raw_state}
    assert job.getState() == expected_state


def test_slurm_job_get_state_returns_unknown_for_unmapped_value():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"JobState": "SOMETHING_ELSE"}
    assert job.getState() == BatchState.UNKNOWN


def test_slurm_job_get_state_returns_held_when_dependency_in_comment(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"JobState": "PENDING"}
    monkeypatch.setattr(job, "getComment", lambda: "Reason: (Dependency)")
    assert job.getState() == BatchState.HELD


def test_slurm_job_get_state_returns_held_when_dependency_in_comment2(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"JobState": "PENDING"}
    monkeypatch.setattr(job, "getComment", lambda: "Reason: (DependencyNeverSatisfied)")
    assert job.getState() == BatchState.HELD


def test_slurm_job_get_state_returns_queued_when_no_dependency(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"JobState": "PENDING"}
    monkeypatch.setattr(job, "getComment", lambda: "Reason: (JobArrayTaskLimit)")
    assert job.getState() == BatchState.QUEUED


def test_slurm_job_get_comment_returns_reason_string():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"Reason": "(Dependency)"}
    assert job.getComment() == "Reason: (Dependency)"


def test_slurm_job_get_comment_returns_none_when_reason_is_none_string():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"Reason": "None"}
    assert job.getComment() is None


def test_slurm_job_get_comment_returns_none_when_reason_missing():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {}
    assert job.getComment() is None


def test_slurm_job_get_estimated_returns_none_when_starttime_none(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "getStartTime", lambda: None)
    assert job.getEstimated() is None


def test_slurm_job_get_estimated_returns_none_when_starttime_string_none(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "getStartTime", lambda: "None")
    assert job.getEstimated() is None


def test_slurm_job_get_estimated_returns_none_when_nodelist_missing(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "getStartTime", lambda: datetime.now())
    job._info = {}
    assert job.getEstimated() is None


def test_slurm_job_get_estimated_returns_none_when_nodelist_contains_none(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "getStartTime", lambda: datetime.now())
    job._info = {"SchedNodeList": "None assigned"}
    assert job.getEstimated() is None


def test_slurm_job_get_estimated_returns_tuple_when_valid(monkeypatch):
    fake_time = datetime.now()
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "getStartTime", lambda: fake_time)
    job._info = {"SchedNodeList": "node123"}
    assert job.getEstimated() == (fake_time, "node123")


def test_slurm_job_get_main_node_returns_batchhost_when_valid():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"BatchHost": "mainnode1"}
    assert job.getMainNode() == "mainnode1"


def test_slurm_job_get_main_node_returns_none_when_batchhost_contains_none(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"BatchHost": "None"}
    monkeypatch.setattr(job, "getNodes", lambda: [])
    assert job.getMainNode() is None


def test_slurm_job_get_main_node_returns_first_node_when_batchhost_missing(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    job._info = {}
    monkeypatch.setattr(job, "getNodes", lambda: ["nodeA", "nodeB"])
    assert job.getMainNode() == "nodeA"


def test_slurm_job_get_main_node_returns_none_when_no_batchhost_and_no_nodes(
    monkeypatch,
):
    job = SlurmJob.__new__(SlurmJob)
    job._info = {}
    monkeypatch.setattr(job, "getNodes", lambda: [])
    assert job.getMainNode() is None


def test_slurm_job_get_nodes_returns_expanded_list(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"NodeList": "node[1-3]"}
    monkeypatch.setattr(
        SlurmJob, "_expandNodeList", lambda _: ["node1", "node2", "node3"]
    )
    assert job.getNodes() == ["node1", "node2", "node3"]


def test_slurm_job_get_nodes_returns_none_when_nodelist_contains_none():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"NodeList": "None"}
    assert job.getNodes() is None


def test_slurm_job_get_nodes_returns_none_when_nodelist_missing():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {}
    assert job.getNodes() is None


def test_slurm_job_get_short_nodes_returns_list_with_nodelist():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"NodeList": "node[1-3]"}
    assert job.getShortNodes() == ["node[1-3]"]


def test_slurm_job_get_short_nodes_returns_none_when_nodelist_contains_none():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"NodeList": "None"}
    assert job.getShortNodes() is None


def test_slurm_job_get_short_nodes_returns_none_when_nodelist_missing():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {}
    assert job.getShortNodes() is None


def test_slurm_job_get_name_returns_name():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"JobName": "test_job"}
    assert job.getName() == "test_job"


@patch("qq_lib.batch.slurm.job.logger.warning")
def test_slurm_job_get_name_returns_placeholder_and_logs_warning(mock_warning):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "123"
    job._info = {}
    result = job.getName()
    assert result == "?????"
    mock_warning.assert_called_once()


def test_slurm_job_get_ncpus_returns_integer_value():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"NumCPUs": "16", "MinCPUsNode": "8", "NumNodes": "2"}
    assert job.getNCPUs() == 16


def test_slurm_job_get_ncpus_returns_min_value_when_range():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"NumCPUs": "8-32", "MinCPUsNode": "8", "NumNodes": "1"}
    assert job.getNCPUs() == 8


def test_slurm_job_get_ncpus_min_cpus_not_defined():
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "321"
    job._info = {"NumCPUs": "16", "NumNodes": "2"}
    assert job.getNCPUs() == 16


def test_slurm_job_get_ncpus_nnodes_not_defined():
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "321"
    job._info = {"NumCPUs": "16", "MinCPUsNode": "8"}
    assert job.getNCPUs() == 16


def test_slurm_job_get_ncpus_returns_min_cpus_if_higher():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"NumCPUs": "1", "MinCPUsNode": "8", "NumNodes": "2"}
    assert job.getNCPUs() == 16


def test_slurm_job_get_ncpus_returns_zero_when_invalid_value():
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "321"
    job._info = {"NumCPUs": "invalid"}
    assert job.getNCPUs() == 0


def test_slurm_job_get_ncpus_returns_zero_when_key_missing():
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "654"
    job._info = {}
    assert job.getNCPUs() == 0


def test_slurm_job_get_ngpus_returns_value_from_gpu_prefix(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "_getTres", lambda: "cpu=8,gpu=2,mem=16G")
    assert job.getNGPUs() == 2


def test_slurm_job_get_ngpus_returns_value_from_gres_gpu_prefix(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "_getTres", lambda: "gres/gpu=4,mem=32G")
    assert job.getNGPUs() == 4


def test_slurm_job_get_ngpus_returns_zero_when_no_gpu_entry(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "_getTres", lambda: "cpu=8,mem=16G")
    assert job.getNGPUs() == 0


def test_slurm_job_get_ngpus_returns_zero_when_empty_tres(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "_getTres", lambda: "")
    assert job.getNGPUs() == 0


def test_slurm_job_get_ngpus_returns_zero_when_gpu_value_invalid(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "_getTres", lambda: "cpu=8,gpu=invalid,mem=16G")
    assert job.getNGPUs() == 0


def test_slurm_job_get_nnodes_returns_integer_value():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"NumNodes": "3"}
    assert job.getNNodes() == 3


def test_slurm_job_get_nnodes_returns_min_value_when_range():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"NumNodes": "2-5"}
    assert job.getNNodes() == 2


def test_slurm_job_get_nnodes_returns_zero_when_invalid_value():
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "321"
    job._info = {"NumNodes": "invalid"}
    assert job.getNNodes() == 0


def test_slurm_job_get_nnodes_returns_zero_when_key_missing():
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "654"
    job._info = {}
    assert job.getNNodes() == 0


def test_slurm_job_get_mem_returns_parsed_size(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "_getTres", lambda: "cpu=8,mem=16G,gres/gpu=1")
    result = job.getMem()
    assert isinstance(result, Size)
    assert result.value == 16 * 1024 * 1024  # in kb


def test_slurm_job_get_mem_returns_parsed_size_mb(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "_getTres", lambda: "cpu=8,mem=800M,gres/gpu=1")
    result = job.getMem()
    assert isinstance(result, Size)
    assert result.value == 800 * 1024  # in kb


@patch("qq_lib.batch.slurm.job.logger.warning")
def test_slurm_job_get_mem_returns_zero_size_when_parsing_fails(
    mock_warning, monkeypatch
):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "321"
    monkeypatch.setattr(job, "_getTres", lambda: "cpu=4,mem=invalid")
    result = job.getMem()
    assert result.value == 0
    mock_warning.assert_called_once()


@patch("qq_lib.batch.slurm.job.logger.warning")
def test_slurm_job_get_mem_returns_zero_size_when_empty_tres(mock_warning, monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "321"
    monkeypatch.setattr(job, "_getTres", lambda: "")
    result = job.getMem()
    assert result.value == 0
    mock_warning.assert_called_once()


@patch("qq_lib.batch.slurm.job.logger.warning")
def test_slurm_job_get_mem_returns_zero_size_when_mem_missing(
    mock_warning, monkeypatch
):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "654"
    monkeypatch.setattr(job, "_getTres", lambda: "cpu=8,gres/gpu=1")
    result = job.getMem()
    assert result.value == 0
    mock_warning.assert_called_once()


def test_slurm_job_get_start_time_returns_parsed_datetime():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"StartTime": "2024-05-10T12:30:00"}
    result = job.getStartTime()
    assert isinstance(result, datetime)
    assert result == datetime(2024, 5, 10, 12, 30, 0)


def test_slurm_job_get_start_time_returns_none_when_invalid_value():
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "123"
    job._info = {"StartTime": "not_a_date"}
    assert job.getStartTime() is None


@pytest.mark.parametrize("val", ["", "none", "n/a", "unknown"])
def test_slurm_job_get_start_time_returns_none_for_placeholder_values(val):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "123"
    job._info = {"StartTime": val}
    assert job.getStartTime() is None


def test_slurm_job_get_start_time_returns_none_when_missing():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {}
    assert job.getStartTime() is None


def test_slurm_job_get_submission_time_returns_parsed_datetime():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"SubmitTime": "2024-05-10T12:00:00"}
    result = job.getSubmissionTime()
    assert isinstance(result, datetime)
    assert result == datetime(2024, 5, 10, 12, 0, 0)


def test_slurm_job_get_submission_time_returns_datetime_min_when_invalid():
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "123"
    job._info = {"SubmitTime": "invalid"}
    assert job.getSubmissionTime() == datetime.min


def test_slurm_job_get_completion_time_returns_parsed_datetime():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"EndTime": "2024-05-10T15:00:00"}
    result = job.getCompletionTime()
    assert isinstance(result, datetime)
    assert result == datetime(2024, 5, 10, 15, 0, 0)


def test_slurm_job_get_completion_time_returns_none_when_invalid():
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "123"
    job._info = {"EndTime": "not_a_date"}
    assert job.getCompletionTime() is None


def test_slurm_job_get_modification_time_returns_completion_time(monkeypatch):
    fake_completion = datetime(2024, 5, 10, 17, 0, 0)
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "getCompletionTime", lambda: fake_completion)
    monkeypatch.setattr(
        job, "getSubmissionTime", lambda: datetime(2024, 5, 10, 12, 0, 0)
    )
    assert job.getModificationTime() == fake_completion


def test_slurm_job_get_modification_time_returns_submission_time_when_no_completion(
    monkeypatch,
):
    fake_submission = datetime(2024, 5, 10, 12, 0, 0)
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "getCompletionTime", lambda: None)
    monkeypatch.setattr(job, "getSubmissionTime", lambda: fake_submission)
    assert job.getModificationTime() == fake_submission


def test_slurm_job_get_user_returns_username_before_parenthesis():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"UserId": "fake_user(1234)"}
    assert job.getUser() == "fake_user"


def test_slurm_job_get_user_returns_full_value_when_no_parenthesis():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"UserId": "user2"}
    assert job.getUser() == "user2"


@patch("qq_lib.batch.slurm.job.logger.warning")
def test_slurm_job_get_user_returns_placeholder_and_logs_warning(mock_warning):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "789"
    job._info = {}
    result = job.getUser()
    assert result == "?????"
    mock_warning.assert_called_once()


def test_slurm_job_get_walltime_returns_correct_timedelta():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"TimeLimit": "2-12:34:56"}
    result = job.getWalltime()
    assert isinstance(result, timedelta)
    assert result == timedelta(days=2, hours=12, minutes=34, seconds=56)


@patch("qq_lib.batch.slurm.job.logger.warning")
def test_slurm_job_get_walltime_returns_zero_and_logs_when_missing(mock_warning):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "123"
    job._info = {}
    result = job.getWalltime()
    assert result == timedelta(0)
    mock_warning.assert_called_once()


@patch("qq_lib.batch.slurm.job.logger.warning")
def test_slurm_job_get_walltime_returns_zero_and_logs_when_invalid(mock_warning):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "456"
    job._info = {"TimeLimit": "invalid"}
    result = job.getWalltime()
    assert result == timedelta(0)
    mock_warning.assert_called_once()


def test_slurm_job_get_queue_returns_partition_value():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"Partition": "default"}
    assert job.getQueue() == "default"


@patch("qq_lib.batch.slurm.job.logger.warning")
def test_slurm_job_get_queue_returns_placeholder_and_logs_warning(mock_warning):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "123"
    job._info = {}
    result = job.getQueue()
    assert result == "?????"
    mock_warning.assert_called_once()


def test_slurm_job_get_util_cpu_returns_none():
    job = SlurmJob.__new__(SlurmJob)
    assert job.getUtilCPU() is None


def test_slurm_job_get_util_mem_returns_none():
    job = SlurmJob.__new__(SlurmJob)
    assert job.getUtilMem() is None


def test_slurm_job_get_exit_code_returns_first_nonzero_code():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"ExitCode": "2:0"}
    assert job.getExitCode() == 2


def test_slurm_job_get_exit_code_returns_signal_when_first_zero():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"ExitCode": "0:9"}
    assert job.getExitCode() == 9


def test_slurm_job_get_exit_code_returns_zero_when_both_zero():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"ExitCode": "0:0"}
    assert job.getExitCode() == 0


def test_slurm_job_get_exit_code_returns_none_when_missing():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {}
    assert job.getExitCode() is None


def test_slurm_job_get_exit_code_returns_none_when_invalid_format():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"ExitCode": "invalid"}
    assert job.getExitCode() is None


def test_slurm_job_get_input_machine_returns_placeholder():
    job = SlurmJob.__new__(SlurmJob)
    assert job.getInputMachine() == "?????"


def test_slurm_job_get_input_dir_returns_resolved_path():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"WorkDir": "/tmp/testdir"}
    resolved = job.getInputDir()
    assert isinstance(resolved, Path)
    assert resolved == Path("/tmp/testdir").resolve()


@patch("qq_lib.batch.slurm.job.logger.warning")
def test_slurm_job_get_input_dir_returns_placeholder_and_logs_warning(mock_warning):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = "123"
    job._info = {}
    result = job.getInputDir()
    assert result == Path("???")
    mock_warning.assert_called_once()


def test_slurm_job_get_info_file_returns_path_when_file_exists(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    fake_path = Path("/tmp/testfile.qqinfo")
    monkeypatch.setattr(job, "getInputDir", lambda: Path("/tmp"))
    monkeypatch.setattr(job, "getName", lambda: "testfile")
    monkeypatch.setattr(
        "qq_lib.batch.slurm.job.CFG.suffixes", MagicMock(qq_info=".qqinfo")
    )
    monkeypatch.setattr(Path, "is_file", lambda _: True)
    result = job.getInfoFile()
    assert result == fake_path


def test_slurm_job_get_info_file_returns_none_when_file_missing(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "getInputDir", lambda: Path("/tmp"))
    monkeypatch.setattr(job, "getName", lambda: "missingfile")
    monkeypatch.setattr(
        "qq_lib.batch.slurm.job.CFG.suffixes", MagicMock(qq_info=".qqinfo")
    )
    monkeypatch.setattr(Path, "is_file", lambda _: False)
    assert job.getInfoFile() is None


def test_slurm_job_get_info_file_returns_none_when_permission_error(monkeypatch):
    job = SlurmJob.__new__(SlurmJob)
    monkeypatch.setattr(job, "getInputDir", lambda: Path("/tmp"))
    monkeypatch.setattr(job, "getName", lambda: "noaccessfile")
    monkeypatch.setattr(
        "qq_lib.batch.slurm.job.CFG.suffixes", MagicMock(qq_info=".qqinfo")
    )

    def raise_permission_error(self):
        _ = self
        raise PermissionError

    monkeypatch.setattr(Path, "is_file", raise_permission_error)
    assert job.getInfoFile() is None


def test_slurm_job_to_yaml_returns_valid_yaml_string():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {
        "JobId": "111111",
        "JobName": "example_job",
        "UserId": "user(1001)",
        "Account": "project-001",
        "JobState": "RUNNING",
        "Partition": "gpu",
        "NodeList": "node01",
        "NumNodes": "1",
        "NumCPUs": "32",
        "SubmitTime": "2025-11-05T11:53:40",
        "StartTime": "2025-11-05T11:54:00",
        "EndTime": "2025-11-05T13:54:00",
        "WorkDir": "/home/user/project",
        "Command": "/home/user/project/run.sh",
        "StdOut": "/home/user/project/slurm-111111.out",
        "StdErr": "/home/user/project/slurm-111111.out",
        "ExitCode": "0:0",
        "ReqTRES": "cpu=32,mem=128G,node=1,gres/gpu=4",
        "AllocTRES": "cpu=32,mem=128G,node=1,gres/gpu=4",
    }

    yaml_str = job.toYaml()

    assert isinstance(yaml_str, str)
    parsed = yaml.load(yaml_str, Loader=yaml.SafeLoader)
    assert parsed == job._info


def test_slurm_job_from_dict():
    info = {"JobName": "example_job", "JobState": "RUNNING"}
    job = SlurmJob.fromDict("12345", info)
    assert isinstance(job, SlurmJob)
    assert job._job_id == "12345"
    assert job._info == info


def test_slurm_job_from_sacct_string_returns_valid_instance():
    fields = [
        "111111",
        "project001",
        "RUNNING",
        "user(1001)",
        "example_job",
        "gpu",
        "/home/user/project",
        "",
        "32",
        "None",
        "cpu=32,mem=128G",
        "0",
        "1",
        "2025-11-05T11:53:40",
        "2025-11-05T11:54:00",
        "2025-11-05T13:54:00",
        "2-00:00:00",
        "node01",
        "None",
        "0:0",
    ]
    sacct_str = "|".join(fields)
    job = SlurmJob.fromSacctString(sacct_str)

    assert isinstance(job, SlurmJob)
    assert job._job_id == "111111"
    assert job._info["JobState"] == "RUNNING"
    assert job._info["NumCPUs"] == "32"
    assert job._info["NumNodes"] == "1"


def test_slurm_job_from_sacct_string_raises_error_when_field_count_invalid():
    bad_str = "too|few|fields"
    with pytest.raises(QQError):
        SlurmJob.fromSacctString(bad_str)


@patch("qq_lib.batch.slurm.job.subprocess.run")
def test_slurm_job_expand_node_list_returns_expanded_list(mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "node01\nnode02\nnode03\n"
    mock_run.return_value = mock_result

    result = SlurmJob._expandNodeList("node[01-03]")
    assert result == ["node01", "node02", "node03"]
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.job.subprocess.run")
@patch("qq_lib.batch.slurm.job.logger.warning")
def test_slurm_job_expand_node_list_returns_unexpanded_on_failure(
    mock_warning, mock_run
):
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "error"
    mock_run.return_value = mock_result

    result = SlurmJob._expandNodeList("node[01-03]")
    assert result == ["node[01-03]"]
    mock_warning.assert_called_once()


def test_slurm_job_get_tres_returns_alloc_tres_when_valid():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"AllocTRES": "cpu=8,mem=16G", "ReqTRES": "cpu=4,mem=8G"}
    assert job._getTres() == "cpu=8,mem=16G"


@pytest.mark.parametrize("invalid_value", ["null", "None", "N/A", ""])
def test_slurm_job_get_tres_falls_back_to_req_tres(invalid_value):
    job = SlurmJob.__new__(SlurmJob)
    job._info = {"AllocTRES": invalid_value, "ReqTRES": "cpu=4,mem=8G"}
    assert job._getTres() == "cpu=4,mem=8G"


def test_slurm_job_get_tres_returns_empty_string_when_both_missing():
    job = SlurmJob.__new__(SlurmJob)
    job._info = {}
    assert job._getTres() == ""


def test_slurm_job_assign_if_allocated_uses_alloc_value():
    info = {"AllocCPUs": "16", "ReqCPUs": "8"}
    SlurmJob._assignIfAllocated(info, "AllocCPUs", "ReqCPUs", "NumCPUs")
    assert info["NumCPUs"] == "16"


@pytest.mark.parametrize("invalid_value", [None, "None", "", "0"])
def test_slurm_job_assign_if_allocated_falls_back_to_req_value(invalid_value):
    info = {"AllocCPUs": invalid_value, "ReqCPUs": "8"}
    SlurmJob._assignIfAllocated(info, "AllocCPUs", "ReqCPUs", "NumCPUs")
    assert info["NumCPUs"] == "8"


def test_slurm_job_assign_if_allocated_falls_back_to_zero_when_req_missing():
    info = {"AllocCPUs": "0"}
    SlurmJob._assignIfAllocated(info, "AllocCPUs", "ReqCPUs", "NumCPUs")
    assert info["NumCPUs"] == "0"


@pytest.mark.parametrize(
    "job_id,expected",
    [
        ("123", [123]),
        ("123.server.org", [123]),
        ("123_45", [123, 45]),
        ("123_45_6.server.org", [123, 45, 6]),
        ("9x", [9]),
    ],
)
def test_slurm_job_get_ids_for_sorting_returns_correct_groups(job_id, expected):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = job_id
    assert job.getIdsForSorting() == expected


@pytest.mark.parametrize("job_id", ["abc123", "_123", "", "x_y_z"])
def test_slurm_job_get_ids_for_sorting_returns_zero_for_invalid(job_id):
    job = SlurmJob.__new__(SlurmJob)
    job._job_id = job_id
    assert job.getIdsForSorting() == [0]
