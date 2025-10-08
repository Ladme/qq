# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: W291

import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.interface import QQBatchInterface
from qq_lib.batch.pbs import QQPBS, PBSJobInfo
from qq_lib.core.constants import QQ_OUT_SUFFIX, SHARED_SUBMIT, SSH_TIMEOUT
from qq_lib.core.error import QQError
from qq_lib.properties.resources import QQResources
from qq_lib.properties.size import Size
from qq_lib.properties.states import BatchState


@pytest.fixture
def sample_dump_file():
    return """
Job Id: 123456.fake-cluster.example.com
    Job_Name = example_job
    Job_Owner = user@EXAMPLE
    resources_used.cpupercent = 100
    resources_used.cput = 01:23:45
    resources_used.diag_messages = '{}'
    resources_used.mem = 102400kb
    resources_used.ncpus = 8
    resources_used.vmem = 102400kb
    resources_used.walltime = 02:00:00
    job_state = R
    queue = gpu
    server = fake-cluster.example.com
    ctime = Sun Sep 21 00:00:00 2025
    depend = afterany:123455.fake-cluster.example.com@fake-cluster.example.com
    Error_Path = /fake/path/job_123456.log
    exec_host = node1/8*8
    exec_host2 = node1.example.com:15002/8*8
    exec_vnode = (node1:ncpus=8:ngpus=1:mem=8192mb:scratch_local=8192mb)
    group_list = examplegroup
    Hold_Types = n
    Join_Path = oe
    Mail_Points = n
    mtime = Sun Sep 21 02:00:00 2025
    Output_Path = /fake/path/job_123456.log
    qtime = Sun Sep 21 00:00:00 2025
    Rerunable = False
    Resource_List.mem = 8gb
    Resource_List.mpiprocs = 8
    Resource_List.ncpus = 8
    Resource_List.ngpus = 1
    Resource_List.nodect = 1
    Resource_List.place = free
    Resource_List.scratch_local = 8gb
    Resource_List.select = 1:ncpus=8:ngpus=1:mpiprocs=8:mem=8gb:scratch_local=8gb:cl_two=true:ompthreads=1:node_owner=everybody
    Resource_List.walltime = 24:00:00
    stime = Sun Sep 21 00:00:00 2025
    session_id = 123456
    jobdir = /fake/home/user
    substate = 42
    Variable_List = QQ_DEBUG=true,QQ_ENV_SET=true,AMS_SITE_SUPPORT=linuxsupport@example.com,PBS_O_LOGNAME=user,PBS_O_QUEUE=gpu,PBS_O_HOST=host.example.com,SCRATCHDIR=/scratch/user/job_123456,SCRATCH=/scratch/user/job_123456,SINGULARITY_TMPDIR=/scratch/user/job_123456,SINGULARITY_CACHEDIR=/scratch/user/job_123456
    etime = Sun Sep 21 00:00:00 2025
    umask = 77
    run_count = 1
    eligible_time = 00:00:00
    project = _pbs_project_default
    Submit_Host = host.example.com
    credential_id = user@EXAMPLE
    credential_validity = Mon Sep 22 06:38:19 2025
"""


def test_parse_pbs_dump_empty_string():
    text = ""
    result = PBSJobInfo._parsePBSDumpToDictionary(text)
    assert result == {}


def test_parse_pbs_dump_real_file(sample_dump_file):
    result = PBSJobInfo._parsePBSDumpToDictionary(sample_dump_file)

    assert isinstance(result, dict)
    assert result["Job_Name"].strip() == "example_job"
    assert result["job_state"].strip() == "R"
    assert (
        result["Resource_List.select"].strip()
        == "1:ncpus=8:ngpus=1:mpiprocs=8:mem=8gb:scratch_local=8gb:cl_two=true:ompthreads=1:node_owner=everybody"
    )
    assert result["jobdir"].strip() == "/fake/home/user"
    assert result["resources_used.cpupercent"].strip() == "100"
    assert "QQ_DEBUG=true,QQ_ENV_SET=true," in result["Variable_List"]
    assert "SINGULARITY_CACHEDIR=/scratch/user/job_123456" in result["Variable_List"]


def test_parse_pbs_dump_nonsense_input():
    text = """
This is not a key-value
Just some random text
= = =
Another line without equal
KEY = 
=VALUE
NORMAL = OK
NOTCONTINUATION
"""
    result = PBSJobInfo._parsePBSDumpToDictionary(text)

    assert result.get("NORMAL") == "OK"

    assert "This is not a key-value" not in result
    assert "KEY" not in result


def test_get_job_state(sample_dump_file):
    pbs_job_info = object.__new__(PBSJobInfo)
    pbs_job_info._info = PBSJobInfo._parsePBSDumpToDictionary(sample_dump_file)

    assert pbs_job_info.getJobState() == BatchState.RUNNING

    pbs_job_info._info["job_state"] = "Q"
    assert pbs_job_info.getJobState() == BatchState.QUEUED

    pbs_job_info._info["job_state"] = "F"
    # no exit code
    assert pbs_job_info.getJobState() == BatchState.FAILED

    pbs_job_info._info["job_state"] = "F"
    pbs_job_info._info["Exit_status"] = " 0 "
    assert pbs_job_info.getJobState() == BatchState.FINISHED

    pbs_job_info._info["job_state"] = "F"
    pbs_job_info._info["Exit_status"] = " 3"
    assert pbs_job_info.getJobState() == BatchState.FAILED

    pbs_job_info._info["job_state"] = "z"
    assert pbs_job_info.getJobState() == BatchState.UNKNOWN


def _make_jobinfo_with_info(info: dict[str, str]) -> PBSJobInfo:
    job = PBSJobInfo.__new__(PBSJobInfo)
    job._job_id = "1234"
    job._info = info
    return job


def test_get_job_comment_present():
    job = _make_jobinfo_with_info({"comment": "This is a test"})
    assert job.getJobComment() == "This is a test"


def test_get_job_comment_missing():
    job = _make_jobinfo_with_info({})
    assert job.getJobComment() is None


def test_get_job_estimated_success():
    raw_time = "Fri Oct  4 15:30:00 2024"
    vnode = "(node01:some_extra:additional_info)"
    job = _make_jobinfo_with_info(
        {"estimated.start_time": raw_time, "estimated.exec_vnode": vnode}
    )

    result = job.getJobEstimated()
    assert isinstance(result, tuple)

    parsed_time, parsed_vnode = result

    expected_time = datetime(2024, 10, 4, 15, 30, 0)
    assert parsed_time == expected_time
    assert parsed_vnode == "node01"


def test_get_job_estimated_success_simple_node_name():
    raw_time = "Fri Oct  4 15:30:00 2024"
    vnode = "node01"
    job = _make_jobinfo_with_info(
        {"estimated.start_time": raw_time, "estimated.exec_vnode": vnode}
    )

    result = job.getJobEstimated()
    assert isinstance(result, tuple)

    parsed_time, parsed_vnode = result

    expected_time = datetime(2024, 10, 4, 15, 30, 0)
    assert parsed_time == expected_time
    assert parsed_vnode == "node01"


def test_get_job_estimated_missing_time():
    job = _make_jobinfo_with_info(
        {"estimated.exec_vnode": "(node01:some_extra:additional_info)"}
    )
    assert job.getJobEstimated() is None


def test_get_job_estimated_missing_vnode():
    raw_time = "Fri Oct  4 15:30:00 2024"
    job = _make_jobinfo_with_info({"estimated.start_time": raw_time})
    assert job.getJobEstimated() is None


def test_get_job_estimated_parses_vnode_correctly():
    raw_time = "Fri Oct  4 15:30:00 2024"
    vnode = "(node02:ncpus=4)"
    job = _make_jobinfo_with_info(
        {"estimated.start_time": raw_time, "estimated.exec_vnode": vnode}
    )
    estimated = job.getJobEstimated()
    assert estimated is not None
    _, parsed_vnode = estimated
    assert parsed_vnode == "node02"


def test_get_job_estimated_multiple_nodes():
    raw_time = "Fri Oct  4 15:30:00 2024"
    vnode = "(node01:some_extra:additional_info)+(node03:something_else:fake_property) +node05  +  node07+(node09)"
    job = _make_jobinfo_with_info(
        {"estimated.start_time": raw_time, "estimated.exec_vnode": vnode}
    )

    result = job.getJobEstimated()
    assert isinstance(result, tuple)

    parsed_time, parsed_vnode = result

    expected_time = datetime(2024, 10, 4, 15, 30, 0)
    assert parsed_time == expected_time
    assert parsed_vnode == "node01 + node03 + node05 + node07 + node09"


def test_get_main_node():
    job = _make_jobinfo_with_info({"exec_host2": "node04.fake.server.org:15002/3*8"})

    assert job.getMainNode() == "node04.fake.server.org"


def test_get_main_node_multiple_nodes():
    job = _make_jobinfo_with_info(
        {
            "exec_host2": "node04.fake.server.org:15002/3*8+node05.fake.server.org:15002/3*8 + node07.fake.server.org:15002/3*8"
        }
    )

    assert job.getMainNode() == "node04.fake.server.org"


def test_get_main_node_none():
    job = _make_jobinfo_with_info({})

    assert job.getMainNode() is None


def test_get_nodes():
    job = _make_jobinfo_with_info(
        {
            "exec_host2": "node04.fake.server.org:15002/3*8+node05.fake.server.org:15002/3*8 + node07.fake.server.org:15002/3*8"
        }
    )

    assert job.getNodes() == [
        "node04.fake.server.org",
        "node05.fake.server.org",
        "node07.fake.server.org",
    ]


def test_clean_node_name():
    assert PBSJobInfo._cleanNodeName("node02") == "node02"
    assert PBSJobInfo._cleanNodeName("(node02:ncpus=4)") == "node02"
    assert (
        PBSJobInfo._cleanNodeName(
            "(node05:ncpus=8:ngpus=1:mem=8388608kb:scratch_local=8388608kb)"
        )
        == "node05"
    )
    assert (
        PBSJobInfo._cleanNodeName(
            "node08:ncpus=8:ngpus=1:mem=8388608kb:scratch_local=8388608kb"
        )
        == "node08"
    )


def test_pbs_job_info_get_job_name_present():
    job = _make_jobinfo_with_info({"Job_Name": "training_job"})
    assert job.getJobName() == "training_job"


def test_pbs_job_info_get_job_name_missing():
    job = _make_jobinfo_with_info({})
    result = job.getJobName()
    assert result == "?????"


def test_pbs_job_info_get_ncpus_present():
    job = _make_jobinfo_with_info({"Resource_List.ncpus": "16"})
    assert job.getNCPUs() == 16


def test_pbs_job_info_get_ncpus_missing():
    job = _make_jobinfo_with_info({})
    result = job.getNCPUs()
    assert result == 0


def test_pbs_job_info_get_ngpus_present():
    job = _make_jobinfo_with_info({"Resource_List.ngpus": "2"})
    assert job.getNGPUs() == 2


def test_pbs_job_info_get_ngpus_missing():
    job = _make_jobinfo_with_info({})
    result = job.getNGPUs()
    assert result == 0


def test_pbs_job_info_get_nnodes_present():
    job = _make_jobinfo_with_info({"Resource_List.nodect": "3"})
    assert job.getNNodes() == 3


def test_pbs_job_info_get_nnodes_missing():
    job = _make_jobinfo_with_info({})
    result = job.getNNodes()
    assert result == 0


def test_pbs_job_info_get_mem_present():
    job = _make_jobinfo_with_info({"Resource_List.mem": "8gb"})
    mem = job.getMem()
    assert isinstance(mem, Size)
    assert mem.value == 8
    assert mem.unit == "gb"


def test_pbs_job_info_get_mem_missing():
    job = _make_jobinfo_with_info({})
    mem = job.getMem()
    # smallest size is 1 kb
    assert mem.value == 1 and mem.unit == "kb"


def test_pbs_job_info_get_mem_invalid_value():
    job = _make_jobinfo_with_info({"Resource_List.mem": "invalid123"})
    mem = job.getMem()
    # smallest size is 1kb
    assert mem.value == 1 and mem.unit == "kb"


def test_pbs_job_info_get_start_time_present():
    raw_time = "Sun Sep 21 03:15:27 2025"
    job = _make_jobinfo_with_info({"stime": raw_time})
    result = job.getStartTime()
    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 9
    assert result.day == 21
    assert result.hour == 3
    assert result.minute == 15
    assert result.second == 27


def test_pbs_job_info_get_start_time_missing():
    job = _make_jobinfo_with_info({})
    result = job.getStartTime()
    assert result is None


def test_pbs_job_info_get_submission_time_present():
    raw_time = "Sun Sep 21 03:15:27 2025"
    job = _make_jobinfo_with_info({"qtime": raw_time})
    result = job.getSubmissionTime()
    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 9
    assert result.day == 21
    assert result.hour == 3
    assert result.minute == 15
    assert result.second == 27


def test_pbs_job_info_get_submission_time_missing():
    job = _make_jobinfo_with_info({})
    result = job.getSubmissionTime()
    assert result == datetime.min


def test_pbs_job_info_get_completion_time_present():
    raw_time = "Sun Sep 21 03:15:27 2025"
    job = _make_jobinfo_with_info({"obittime": raw_time})
    result = job.getCompletionTime()
    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 9
    assert result.day == 21
    assert result.hour == 3
    assert result.minute == 15
    assert result.second == 27


def test_pbs_job_info_get_completion_time_missing():
    job = _make_jobinfo_with_info({})
    result = job.getCompletionTime()
    assert result is None


def test_pbs_job_info_get_user_present():
    job = _make_jobinfo_with_info({"Job_Owner": "user@CLUSTER"})
    assert job.getUser() == "user"


def test_pbs_job_info_get_user_missing():
    job = _make_jobinfo_with_info({})
    result = job.getUser()
    assert result == "?????"


def test_pbs_job_info_get_walltime_valid():
    job = _make_jobinfo_with_info({"Resource_List.walltime": "12:35:13"})
    result = job.getWalltime()
    assert result == timedelta(hours=12, minutes=35, seconds=13)


def test_pbs_job_info_get_walltime_missing():
    job = _make_jobinfo_with_info({})
    result = job.getWalltime()
    assert result == timedelta(0)


def test_pbs_job_info_get_walltime_invalid():
    job = _make_jobinfo_with_info({"Resource_List.walltime": "not-a-time"})
    result = job.getWalltime()
    assert result == timedelta(0)


def test_pbs_job_info_get_queue_present():
    job = _make_jobinfo_with_info({"queue": "gpu"})
    assert job.getQueue() == "gpu"


def test_pbs_job_info_get_queue_missing():
    job = _make_jobinfo_with_info({})
    result = job.getQueue()
    assert result == "?????"


def test_pbs_job_info_get_util_cpu_valid():
    job = _make_jobinfo_with_info(
        {"resources_used.cpupercent": "200", "Resource_List.ncpus": "4"}
    )
    assert job.getUtilCPU() == 50


def test_pbs_job_info_get_util_cpu_missing():
    job = _make_jobinfo_with_info({})
    result = job.getUtilCPU()
    assert result is None


def test_pbs_job_info_get_util_cpu_invalid():
    job = _make_jobinfo_with_info(
        {"resources_used.cpupercent": "abc", "Resource_List.ncpus": "4"}
    )
    result = job.getUtilCPU()
    assert result is None


def test_pbs_job_info_get_util_mem_valid():
    job = _make_jobinfo_with_info(
        {"resources_used.mem": "1048576kb", "Resource_List.mem": "8gb"}
    )
    assert job.getUtilMem() == 12


def test_pbs_job_info_get_util_mem_zero():
    job = _make_jobinfo_with_info(
        {"resources_used.mem": "0b", "Resource_List.mem": "8gb"}
    )
    assert job.getUtilMem() == 0


def test_pbs_job_info_get_util_mem_missing():
    job = _make_jobinfo_with_info({})
    result = job.getUtilMem()
    assert result is None


def test_pbs_job_info_get_util_mem_invalid():
    job = _make_jobinfo_with_info(
        {"resources_used.mem": "invalid", "Resource_List.mem": "8gb"}
    )
    result = job.getUtilMem()
    assert result is None


def test_pbs_job_info_get_exit_code_valid():
    job = _make_jobinfo_with_info({"Exit_status": "0"})
    assert job.getExitCode() == 0


def test_pbs_job_info_get_exit_code_valid_nonzero():
    job = _make_jobinfo_with_info({"Exit_status": " 2 "})
    assert job.getExitCode() == 2


def test_pbs_job_info_get_exit_code_invalid():
    job = _make_jobinfo_with_info({"Exit_status": "oops"})
    result = job.getExitCode()
    assert result is None


def test_pbs_job_info_get_exit_code_missing():
    job = _make_jobinfo_with_info({})
    assert job.getExitCode() is None


def test_from_dict_creates_instance():
    info = {"Job_Name": "abc"}
    job = PBSJobInfo.fromDict("job123", info)
    assert isinstance(job, PBSJobInfo)
    assert job._job_id == "job123"
    assert job._info is info


@pytest.fixture
def resources():
    return QQResources(
        nnodes=1, mem_per_cpu="1gb", ncpus=4, work_dir="scratch_local", work_size="16gb"
    )


def test_translate_kill_force():
    job_id = "123"
    cmd = QQPBS._translateKillForce(job_id)
    assert cmd == f"qdel -W force {job_id}"


def test_translate_kill():
    job_id = "123"
    cmd = QQPBS._translateKill(job_id)
    assert cmd == f"qdel {job_id}"


def test_navigate_success(tmp_path):
    directory = tmp_path

    with patch("subprocess.run") as mock_run:
        QQPBS.navigateToDestination("fake.host.org", directory)
        # check that subprocess was called properly
        mock_run.assert_called_once_with(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                f"-o ConnectTimeout={SSH_TIMEOUT}",
                "fake.host.org",
                "-t",
                f"cd {directory} || exit {QQBatchInterface.CD_FAIL} && exec bash -l",
            ]
        )

        # should not raise


def test_shared_guard_sets_env_var():
    os.environ.pop(SHARED_SUBMIT, None)

    # patch isShared to return True
    with patch.object(QQPBS, "isShared", return_value=True):
        QQPBS._sharedGuard(QQResources(work_dir="scratch_local"))
        assert os.environ.get(SHARED_SUBMIT) == "true"

    # clean up
    os.environ.pop(SHARED_SUBMIT, None)


def test_shared_guard_does_not_set_env_var():
    os.environ.pop(SHARED_SUBMIT, None)

    # patch isShared to return False
    with patch.object(QQPBS, "isShared", return_value=False):
        QQPBS._sharedGuard(QQResources(work_dir="scratch_local"))
        assert SHARED_SUBMIT not in os.environ


def test_shared_guard_jobdir_does_not_raise():
    os.environ.pop(SHARED_SUBMIT, None)

    # patch isShared to return True
    with patch.object(QQPBS, "isShared", return_value=True):
        QQPBS._sharedGuard(QQResources(work_dir="job_dir"))
        assert os.environ.get(SHARED_SUBMIT) == "true"

    # clean up
    os.environ.pop(SHARED_SUBMIT, None)


def test_shared_guard_jobdir_raises():
    os.environ.pop(SHARED_SUBMIT, None)

    # patch isShared to return False
    with (
        patch.object(QQPBS, "isShared", return_value=False),
        pytest.raises(
            QQError,
            match="Job was requested to run directly in the submission directory",
        ),
    ):
        QQPBS._sharedGuard(QQResources(work_dir="job_dir"))
        assert SHARED_SUBMIT not in os.environ


def test_sync_with_exclusions_shared_storage_sets_local(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = [Path("file1"), Path("file2")]

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync:
        QQPBS.syncWithExclusions(src_dir, dest_dir, "host1", "host2", exclude_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, exclude_files)

    monkeypatch.delenv(SHARED_SUBMIT)


def test_sync_with_exclusions_local_src(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = [Path("file1")]
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # source is local, destination is remote
        QQPBS.syncWithExclusions(
            src_dir, dest_dir, local_host, "remotehost", exclude_files
        )
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, None, "remotehost", exclude_files
        )


def test_sync_with_exclusions_local_dest(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = []
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # destination is local, source is remote
        QQPBS.syncWithExclusions(
            src_dir, dest_dir, "remotehost", local_host, exclude_files
        )
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, "remotehost", None, exclude_files
        )


def test_sync_with_exclusions_one_remote(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = None
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # source local, destination local -> uses None
        QQPBS.syncWithExclusions(src_dir, dest_dir, None, local_host, exclude_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, exclude_files)


def test_sync_with_exclusions_both_remote_raises(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = None

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch("socket.gethostname", return_value="localhost"),
        pytest.raises(QQError, match="cannot be both remote"),
    ):
        # both source and destination are remote and job directory is not shared
        QQPBS.syncWithExclusions(src_dir, dest_dir, "remote1", "remote2", exclude_files)


def test_sync_selected_shared_storage_sets_local(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = [Path("file1"), Path("file2")]

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with patch.object(QQBatchInterface, "syncSelected") as mock_sync:
        QQPBS.syncSelected(src_dir, dest_dir, "host1", "host2", include_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, include_files)

    monkeypatch.delenv(SHARED_SUBMIT)


def test_sync_selected_local_src(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = [Path("file1")]
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, local_host, "remotehost", include_files)
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, None, "remotehost", include_files
        )


def test_sync_selected_local_dest(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = []
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, "remotehost", local_host, include_files)
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, "remotehost", None, include_files
        )


def test_sync_selected_one_remote(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = None
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, None, local_host, include_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, include_files)


def test_sync_selected_both_remote_raises(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = None

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch("socket.gethostname", return_value="localhost"),
        pytest.raises(QQError, match="cannot be both remote"),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, "remote1", "remote2", include_files)


def test_read_remote_file_shared_storage(tmp_path, monkeypatch):
    file_path = tmp_path / "testfile.txt"
    content = "Hello, QQ!"
    file_path.write_text(content)

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    result = QQPBS.readRemoteFile("remotehost", file_path)
    assert result == content

    monkeypatch.delenv(SHARED_SUBMIT)


def test_read_remote_file_shared_storage_file_missing(tmp_path, monkeypatch):
    file_path = tmp_path / "nonexistent.txt"

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with pytest.raises(QQError, match="Could not read file"):
        QQPBS.readRemoteFile("remotehost", file_path)

    monkeypatch.delenv(SHARED_SUBMIT)


def test_read_remote_file_remote():
    file_path = Path("/remote/file.txt")
    with patch.object(
        QQBatchInterface, "readRemoteFile", return_value="data"
    ) as mock_read:
        result = QQPBS.readRemoteFile("remotehost", file_path)
        mock_read.assert_called_once_with("remotehost", file_path)
        assert result == "data"


def test_write_remote_file_shared_storage(tmp_path, monkeypatch):
    file_path = tmp_path / "output.txt"
    content = "Test content"

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    QQPBS.writeRemoteFile("remotehost", file_path, content)
    assert file_path.read_text() == content


def test_write_remote_file_shared_storage_exception(tmp_path, monkeypatch):
    # using a directory instead of a file to cause write_text to fail
    dir_path = tmp_path / "dir"
    dir_path.mkdir()

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with pytest.raises(QQError, match="Could not write file"):
        QQPBS.writeRemoteFile("remotehost", dir_path, "content")


def test_write_remote_file_remote():
    file_path = Path("/remote/output.txt")
    content = "data"

    with patch.object(QQBatchInterface, "writeRemoteFile") as mock_write:
        QQPBS.writeRemoteFile("remotehost", file_path, content)
        mock_write.assert_called_once_with("remotehost", file_path, content)


def test_make_remote_dir_shared_storage(tmp_path, monkeypatch):
    dir_path = tmp_path / "newdir"

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    QQPBS.makeRemoteDir("remotehost", dir_path)

    assert dir_path.exists() and dir_path.is_dir()


def test_make_remote_dir_shared_storage_exception(tmp_path, monkeypatch):
    file_path = tmp_path / "conflict"
    file_path.write_text("dummy")

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with pytest.raises(QQError, match="Could not create a directory"):
        QQPBS.makeRemoteDir("remotehost", file_path)


def test_make_remote_dir_shared_storage_already_exists_ok(tmp_path, monkeypatch):
    dir_path = tmp_path / "newdir"
    dir_path.mkdir()

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    # ignore that the directory already exists
    QQPBS.makeRemoteDir("remotehost", dir_path)

    assert dir_path.exists() and dir_path.is_dir()


def test_make_remote_dir_remote():
    dir_path = Path("/remote/newdir")

    with patch.object(QQBatchInterface, "makeRemoteDir") as mock_make:
        QQPBS.makeRemoteDir("remotehost", dir_path)
        mock_make.assert_called_once_with("remotehost", dir_path)


def test_list_remote_dir_shared_storage(tmp_path, monkeypatch):
    (tmp_path / "file1.txt").write_text("one")
    (tmp_path / "file2.txt").write_text("two")
    (tmp_path / "subdir").mkdir()

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    result = QQPBS.listRemoteDir("remotehost", tmp_path)

    result_names = sorted([p.name for p in result])
    assert result_names == ["file1.txt", "file2.txt", "subdir"]


def test_list_remote_dir_shared_storage_exception(tmp_path, monkeypatch):
    # use a file instead of directory -> .iterdir() should fail
    bad_path = tmp_path / "notadir"
    bad_path.write_text("oops")

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with pytest.raises(QQError, match="Could not list a directory"):
        QQPBS.listRemoteDir("remotehost", bad_path)


def test_list_remote_dir_remote():
    dir_path = Path("/remote/dir")

    with patch.object(QQBatchInterface, "listRemoteDir") as mock_list:
        QQPBS.listRemoteDir("remotehost", dir_path)
        mock_list.assert_called_once_with("remotehost", dir_path)


def test_move_remote_files_shared_storage(tmp_path, monkeypatch):
    src1 = tmp_path / "file1.txt"
    src2 = tmp_path / "file2.txt"
    src1.write_text("one")
    src2.write_text("two")

    dst_dir = tmp_path / "dest"
    dst_dir.mkdir()
    dst1 = tmp_path / "dest1.txt"
    dst2 = dst_dir / "dest2.txt"

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    QQPBS.moveRemoteFiles("remotehost", [src1, src2], [dst1, dst2])

    # check that files were moved
    assert dst1.exists() and dst1.read_text() == "one"
    assert dst2.exists() and dst2.read_text() == "two"
    assert not src1.exists()
    assert not src2.exists()


def test_move_remote_files_shared_storage_exception(tmp_path, monkeypatch):
    bad_src = tmp_path / "dir"
    bad_src.mkdir()
    dst = tmp_path / "dest"

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    # normally shutil.move would move a directory,
    # so we force an error by making the destination a file
    (dst).write_text("dummy")

    with pytest.raises(Exception):
        QQPBS.moveRemoteFiles("remotehost", [bad_src], [dst])


def test_move_remote_files_length_mismatch(tmp_path, monkeypatch):
    src = tmp_path / "file1.txt"
    src.write_text("data")
    dst1 = tmp_path / "dest1.txt"
    dst2 = tmp_path / "dest2.txt"

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with pytest.raises(QQError, match="must have the same length"):
        QQPBS.moveRemoteFiles("remotehost", [src], [dst1, dst2])


def test_move_remote_files_remote():
    src = Path("/remote/file.txt")
    dst = Path("/remote/dest.txt")

    with patch.object(QQBatchInterface, "moveRemoteFiles") as mock_move:
        QQPBS.moveRemoteFiles("remotehost", [src], [dst])
        mock_move.assert_called_once_with("remotehost", [src], [dst])


def test_translate_work_dir_job_dir_returns_none():
    res = QQResources(nnodes=1, work_dir="job_dir")
    assert QQPBS._translateWorkDir(res) is None


def test_translate_work_dir_scratch_shm_returns_true_string():
    res = QQResources(nnodes=3, work_dir="scratch_shm")
    assert QQPBS._translateWorkDir(res) == "scratch_shm=true"


def test_translate_work_dir_work_size_divided_by_nnodes():
    res = QQResources(nnodes=2, work_dir="scratch_local", work_size="7mb")
    result = QQPBS._translateWorkDir(res)
    assert result == "scratch_local=4mb"


def test_translate_work_dir_work_size_per_cpu_and_ncpus():
    res = QQResources(
        nnodes=4, ncpus=5, work_dir="scratch_local", work_size_per_cpu="3mb"
    )
    result = QQPBS._translateWorkDir(res)
    # 3mb * 5 = 15mb, divided by 4 nodes = 4mb
    assert result == "scratch_local=4mb"


def test_translate_work_dir_missing_work_size_raises():
    res = QQResources(nnodes=2, ncpus=4, work_dir="scratch_local")
    with pytest.raises(QQError, match="work-size"):
        QQPBS._translateWorkDir(res)


def test_translate_work_dir_missing_ncpus_with_work_size_per_cpu_raises():
    res = QQResources(nnodes=2, work_dir="scratch_local", work_size_per_cpu="3mb")
    with pytest.raises(QQError, match="work-size"):
        QQPBS._translateWorkDir(res)


def test_translate_per_chunk_resources_nnones_missing_raises():
    res = QQResources(nnodes=None, ncpus=2, mem="4mb")
    with pytest.raises(QQError, match="nnodes"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_nnones_zero_raises():
    res = QQResources(nnodes=0, ncpus=2, mem="4mb")
    with pytest.raises(QQError, match="nnodes"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_ncpus_not_divisible_raises():
    res = QQResources(nnodes=3, ncpus=4, mem="4mb")
    with pytest.raises(QQError, match="ncpus"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_ngpus_not_divisible_raises():
    res = QQResources(nnodes=2, ncpus=2, ngpus=3, mem="4mb")
    with pytest.raises(QQError, match="ngpus"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_mem_division():
    res = QQResources(nnodes=2, ncpus=4, mem="7mb", work_dir="job_dir")
    result = QQPBS._translatePerChunkResources(res)
    assert "ncpus=2" in result
    assert "mem=4mb" in result


def test_translate_per_chunk_resources_mem_per_cpu_used():
    res = QQResources(nnodes=2, ncpus=4, mem_per_cpu="2mb", work_dir="job_dir")
    result = QQPBS._translatePerChunkResources(res)
    # 2mb * 4 / 2 = 4mb
    assert "mem=4mb" in result


def test_translate_per_chunk_resources_ngpus_included():
    res = QQResources(nnodes=3, ncpus=9, mem="8mb", ngpus=6, work_dir="job_dir")
    result = QQPBS._translatePerChunkResources(res)
    assert "ngpus=2" in result


def test_translate_per_chunk_resources_work_dir_translated():
    res = QQResources(
        nnodes=2, ncpus=4, mem="8mb", work_dir="scratch_local", work_size="1mb"
    )
    result = QQPBS._translatePerChunkResources(res)
    assert "scratch_local=512kb" in result


def test_translate_per_chunk_resources_missing_memory_raises():
    res = QQResources(nnodes=2, ncpus=4)
    with pytest.raises(QQError, match="mem"):
        QQPBS._translatePerChunkResources(res)


def test_parse_queue_info_empty_text_returns_empty_dict():
    text = ""
    result = QQPBS._parseQueueInfoToDictionary(text)
    assert result == {}


def test_parse_queue_info_only_non_default_lines_ignored():
    text = """
queue_type = Execution
Priority = 75
total_jobs = 308
"""
    result = QQPBS._parseQueueInfoToDictionary(text)
    assert result == {}


def test_parse_queue_info_extracts_default_resources():
    text = """
resources_max.ngpus = 99
resources_max.walltime = 24:00:00
resources_min.mem = 50mb
resources_default.ngpus = 1
resources_default.walltime = 12:00:00
resources_default.mem = 5gb
"""
    result = QQPBS._parseQueueInfoToDictionary(text)
    expected = {
        "ngpus": "1",
        "walltime": "12:00:00",
        "mem": "5gb",
    }
    assert result == expected


def test_parse_queue_info_ignores_extra_spaces():
    text = """
resources_default.ngpus =    2
resources_default.mem   = 10gb
"""
    result = QQPBS._parseQueueInfoToDictionary(text)
    expected = {
        "ngpus": "2",
        "mem": "10gb",
    }
    assert result == expected


def test_parse_queue_info_multiple_default_resources():
    text = """
resources_default.mem = 8gb
resources_default.ncpus = 16
resources_default.ngpus = 4
"""
    result = QQPBS._parseQueueInfoToDictionary(text)
    expected = {
        "mem": "8gb",
        "ncpus": "16",
        "ngpus": "4",
    }
    assert result == expected


def test_parse_queue_info_ignores_non_resource_default_lines():
    text = """
comment = Example queue
resources_default.mem = 2gb
enabled = True
"""
    result = QQPBS._parseQueueInfoToDictionary(text)
    expected = {"mem": "2gb"}
    assert result == expected


@pytest.mark.parametrize("queue_name", ["gpu", "cpu"])
def test_get_default_queue_resources_success(queue_name):
    mock_output = """
resources_default.mem = 4gb
resources_default.ncpus = 16
resources_default.ngpus = 2
resources_default.walltime = 12:00:00
resources_default.unknown_field = ignored
"""
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = mock_output

    with patch("qq_lib.batch.pbs.subprocess.run", return_value=mock_result) as mock_run:
        res = QQPBS._getDefaultQueueResources(queue_name)

    mock_run.assert_called_once()
    assert isinstance(res, QQResources)
    assert str(res.mem) == "4gb"
    assert res.ncpus == 16
    assert res.ngpus == 2
    assert res.walltime == "12:00:00"
    assert not hasattr(res, "unknown_field")


def test_get_default_queue_resources_failure_returns_empty():
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stdout = ""

    with patch("qq_lib.batch.pbs.subprocess.run", return_value=mock_result) as mock_run:
        res = QQPBS._getDefaultQueueResources("nonexistent_queue")

    mock_run.assert_called_once()

    assert isinstance(res, QQResources)
    for f in res.__dataclass_fields__:
        assert getattr(res, f) is None


def test_get_default_queue_resources_calls_parse_queue_info():
    mock_output = "resources_default.ncpus = 8\nresources_default.mem = 2gb\n"
    mock_result = MagicMock(returncode=0, stdout=mock_output)

    with (
        patch("qq_lib.batch.pbs.subprocess.run", return_value=mock_result),
        patch.object(
            QQPBS,
            "_parseQueueInfoToDictionary",
            wraps=QQPBS._parseQueueInfoToDictionary,
        ) as mock_parse,
    ):
        res = QQPBS._getDefaultQueueResources("gpu")
        mock_parse.assert_called_once_with(mock_output)
        assert res.ncpus == 8
        assert str(res.mem) == "2gb"


def test_translate_submit_minimal_fields():
    res = QQResources(nnodes=1, ncpus=1, mem="1gb", work_dir="job_dir")
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l ncpus=1,mem=1gb script.sh"
    )


def test_translate_submit_multiple_nodes():
    res = QQResources(nnodes=4, ncpus=8, mem="1gb", work_dir="job_dir")
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l select=4:ncpus=2:mem=256mb -l place=scatter script.sh"
    )


def test_translate_submit_with_walltime():
    res = QQResources(
        nnodes=1, ncpus=2, mem="2gb", walltime="1d24m121s", work_dir="job_dir"
    )
    assert (
        QQPBS._translateSubmit(res, "queue", "script.sh", "job")
        == f"qsub -N job -q queue -j eo -e job{QQ_OUT_SUFFIX} -V -l ncpus=2,mem=2gb -l walltime=24:26:01 script.sh"
    )


def test_translate_submit_with_walltime2():
    res = QQResources(
        nnodes=1, ncpus=2, mem="2gb", walltime="12:30:15", work_dir="job_dir"
    )
    assert (
        QQPBS._translateSubmit(res, "queue", "script.sh", "job")
        == f"qsub -N job -q queue -j eo -e job{QQ_OUT_SUFFIX} -V -l ncpus=2,mem=2gb -l walltime=12:30:15 script.sh"
    )


def test_translate_submit_work_dir_scratch_shm():
    res = QQResources(nnodes=1, ncpus=1, mem="8gb", work_dir="scratch_shm")
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l ncpus=1,mem=8gb,scratch_shm=true script.sh"
    )


def test_translate_submit_scratch_local_work_size():
    res = QQResources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_local", work_size="16gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l select=2:ncpus=1:mem=2gb:scratch_local=8gb -l place=scatter script.sh"
    )


def test_translate_submit_scratch_ssd_work_size():
    res = QQResources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_ssd", work_size="16gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l select=2:ncpus=1:mem=2gb:scratch_ssd=8gb -l place=scatter script.sh"
    )


def test_translate_submit_scratch_shared_work_size():
    res = QQResources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_shared", work_size="16gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l select=2:ncpus=1:mem=2gb:scratch_shared=8gb -l place=scatter script.sh"
    )


def test_translate_submit_work_size_per_cpu():
    res = QQResources(
        nnodes=1, ncpus=8, mem="4gb", work_dir="scratch_local", work_size_per_cpu="2gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l ncpus=8,mem=4gb,scratch_local=16gb script.sh"
    )


def test_translate_submit_work_size_per_cpu_multiple_nodes():
    res = QQResources(
        nnodes=3, ncpus=3, mem="4gb", work_dir="scratch_local", work_size_per_cpu="2gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l select=3:ncpus=1:mem=2gb:scratch_local=2gb -l place=scatter script.sh"
    )


def test_translate_submit_mem_per_cpu():
    res = QQResources(
        nnodes=1, ncpus=4, mem_per_cpu="2gb", work_dir="scratch_local", work_size="10gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l ncpus=4,mem=8gb,scratch_local=10gb script.sh"
    )


def test_translate_submit_mem_per_cpu_multiple_nodes():
    res = QQResources(
        nnodes=2, ncpus=4, mem_per_cpu="2gb", work_dir="scratch_local", work_size="20gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l select=2:ncpus=2:mem=4gb:scratch_local=10gb -l place=scatter script.sh"
    )


def test_translate_submit_mem_per_cpu_and_work_size_per_cpu():
    res = QQResources(
        nnodes=1,
        ncpus=4,
        mem_per_cpu="2gb",
        work_dir="scratch_local",
        work_size_per_cpu="5gb",
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l ncpus=4,mem=8gb,scratch_local=20gb script.sh"
    )


def test_translate_submit_mem_per_cpu_and_work_size_per_cpu_multiple_nodes():
    res = QQResources(
        nnodes=2,
        ncpus=4,
        mem_per_cpu="2gb",
        work_dir="scratch_local",
        work_size_per_cpu="5gb",
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V -l select=2:ncpus=2:mem=4gb:scratch_local=10gb -l place=scatter script.sh"
    )


def test_translate_submit_with_props():
    res = QQResources(
        nnodes=1,
        ncpus=1,
        mem="1gb",
        props={"vnode": "my_node", "infiniband": "true"},
        work_dir="job_dir",
    )
    assert (
        QQPBS._translateSubmit(res, "queue", "script.sh", "job")
        == f"qsub -N job -q queue -j eo -e job{QQ_OUT_SUFFIX} -V -l ncpus=1,mem=1gb,vnode=my_node,infiniband=true script.sh"
    )


def test_translate_submit_complex_case():
    res = QQResources(
        nnodes=3,
        ncpus=6,
        mem="5gb",
        ngpus=3,
        walltime="1h30m",
        work_dir="scratch_local",
        work_size_per_cpu="2gb",
        props={"cl_cluster": "true"},
    )
    assert QQPBS._translateSubmit(res, "gpu", "myscript.sh", "job") == (
        f"qsub -N job -q gpu -j eo -e job{QQ_OUT_SUFFIX} -V "
        f"-l select=3:ncpus=2:mem=2gb:ngpus=1:scratch_local=4gb:cl_cluster=true "
        f"-l walltime=1:30:00 -l place=scatter myscript.sh"
    )


def test_transform_resources_job_dir_warns_and_sets_work_dir():
    provided = QQResources(work_dir="job_dir", work_size="10gb")
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        patch("qq_lib.batch.pbs.logger.warning") as mock_warning,
    ):
        res = QQPBS.transformResources(
            "gpu", QQResources(work_dir="job_dir", work_size="10gb")
        )

    assert res.work_dir == "job_dir"

    called_args = mock_warning.call_args[0]
    assert "Setting work-size is not supported" in called_args[0]
    assert "job_dir" in called_args[0]


def test_transform_resources_scratch_shm_warns_and_clears_work_size():
    provided = QQResources(work_dir="scratch_shm", work_size="10gb")
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        patch("qq_lib.batch.pbs.logger.warning") as mock_warning,
    ):
        res = QQPBS.transformResources(
            "gpu", QQResources(work_dir="scratch_shm", work_size="10gb")
        )

    assert res.work_dir == "scratch_shm"
    assert res.work_size is None

    called_args = mock_warning.call_args[0]
    assert "Setting work-size is not supported" in called_args[0]
    assert "scratch_shm" in called_args[0]


def test_transform_resources_supported_scratch():
    for scratch in QQPBS.SUPPORTED_SCRATCHES:
        provided = QQResources(work_dir=scratch, work_size="10gb")
        with (
            patch.object(
                QQPBS, "_getDefaultQueueResources", return_value=QQResources()
            ),
            patch.object(
                QQPBS, "_getDefaultServerResources", return_value=QQResources()
            ),
            patch.object(QQResources, "mergeResources", return_value=provided),
        ):
            res = QQPBS.transformResources(
                "gpu", QQResources(work_dir=scratch, work_size="10gb")
            )

        assert res.work_dir == scratch


def test_transform_resources_supported_scratch_unnormalized():
    for scratch in QQPBS.SUPPORTED_SCRATCHES:
        provided = QQResources(
            work_dir=scratch.upper().replace("_", "-"), work_size="10gb"
        )
        with (
            patch.object(
                QQPBS, "_getDefaultQueueResources", return_value=QQResources()
            ),
            patch.object(
                QQPBS, "_getDefaultServerResources", return_value=QQResources()
            ),
            patch.object(QQResources, "mergeResources", return_value=provided),
        ):
            res = QQPBS.transformResources(
                "gpu",
                QQResources(
                    work_dir=scratch.upper().replace("_", "-"), work_size="10gb"
                ),
            )

        assert res.work_dir == scratch


def test_transform_resources_unknown_work_dir_raises():
    provided = QQResources(work_dir="unknown_scratch")
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        pytest.raises(QQError, match="Unknown working directory type specified"),
    ):
        QQPBS.transformResources("gpu", QQResources(work_dir="unknown_scratch"))


def test_transform_resources_missing_work_dir_raises():
    provided = QQResources(work_dir=None)
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        pytest.raises(
            QQError, match="Work-dir is not set after filling in default attributes"
        ),
    ):
        QQPBS.transformResources("gpu", QQResources())


@pytest.fixture
def sample_multi_dump_file():
    return """Job Id: 123456.fake-cluster.example.com
    Job_Name = example_job_1
    Job_Owner = user@EXAMPLE
    resources_used.cpupercent = 50
    resources_used.ncpus = 4
    job_state = R
    queue = gpu

Job Id: 123457.fake-cluster.example.com
    Job_Name = example_job_2
    Job_Owner = user@EXAMPLE
    resources_used.cpupercent = 75
    resources_used.ncpus = 8
    job_state = Q
    queue = cpu

Job Id: 123458.fake-cluster.example.com
    Job_Name = example_job_3
    Job_Owner = user@EXAMPLE
    resources_used.cpupercent = 100
    resources_used.ncpus = 16
    job_state = H
    queue = gpu
"""


@pytest.mark.parametrize(
    "text,expected_ids,expected_names",
    [
        (
            """Job Id: 1
            Job_Name = job1
            job_state = R

            Job Id: 2
            Job_Name = job2
            job_state = Q
            """,
            ["1", "2"],
            ["job1", "job2"],
        ),
        (
            """Job Id: single_job
            Job_Name = only_job
            job_state = R
            """,
            ["single_job"],
            ["only_job"],
        ),
    ],
)
def test_parse_multi_pbs_dump_to_dictionaries_success(
    text, expected_ids, expected_names
):
    jobs = PBSJobInfo._parseMultiPBSDumpToDictionaries(text)
    assert len(jobs) == len(expected_ids)
    for (info, job_id), exp_id, exp_name in zip(jobs, expected_ids, expected_names):
        assert job_id == exp_id
        assert info["Job_Name"] == exp_name


@pytest.mark.parametrize(
    "text",
    [
        "No Job Id here\nJob_Name = broken",
    ],
)
def test_parse_multi_pbs_dump_to_dictionaries_invalid_input(text):
    with pytest.raises(
        QQError, match="Invalid PBS dump format. Could not extract job id from"
    ):
        PBSJobInfo._parseMultiPBSDumpToDictionaries(text)


@pytest.mark.parametrize(
    "text",
    ["", "\t   ", "\n\n"],
)
def test_parse_multi_pbs_dump_to_dictionaries_empty(text):
    assert PBSJobInfo._parseMultiPBSDumpToDictionaries(text) == []


def test_parse_multi_pbs_dump_to_dictionaries_preserves_multiple_jobs(
    sample_multi_dump_file,
):
    jobs = PBSJobInfo._parseMultiPBSDumpToDictionaries(sample_multi_dump_file)
    assert len(jobs) == 3

    expected_ids = [
        "123456.fake-cluster.example.com",
        "123457.fake-cluster.example.com",
        "123458.fake-cluster.example.com",
    ]
    expected_names = ["example_job_1", "example_job_2", "example_job_3"]
    expected_states = ["R", "Q", "H"]

    for (info, job_id), exp_id, exp_name, exp_state in zip(
        jobs, expected_ids, expected_names, expected_states
    ):
        assert job_id == exp_id
        assert info["Job_Name"] == exp_name
        assert info["job_state"] == exp_state


def test_get_jobs_info_using_command_success(sample_multi_dump_file):
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0, stdout=sample_multi_dump_file, stderr=""
        )

        jobs = QQPBS._getJobsInfoUsingCommand("fake command - unused")

        assert len(jobs) == 3
        assert all(isinstance(job, PBSJobInfo) for job in jobs)

        expected_ids = [
            "123456.fake-cluster.example.com",
            "123457.fake-cluster.example.com",
            "123458.fake-cluster.example.com",
        ]
        assert [job._job_id for job in jobs] == expected_ids  # ty: ignore[unresolved-attribute]

        assert [job._info["Job_Name"] for job in jobs] == [  # ty: ignore[unresolved-attribute]
            "example_job_1",
            "example_job_2",
            "example_job_3",
        ]
        assert [job._info["job_state"] for job in jobs] == [  # ty: ignore[unresolved-attribute]
            "R",
            "Q",
            "H",
        ]

        mock_run.assert_called_once_with(
            ["bash"],
            input="fake command - unused",
            text=True,
            check=False,
            capture_output=True,
        )


def test_get_jobs_info_using_command_nonzero_returncode():
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Some error occurred"
        )
        with pytest.raises(
            QQError,
            match="Could not retrieve information about jobs: Some error occurred",
        ):
            QQPBS._getJobsInfoUsingCommand("will not be used")
